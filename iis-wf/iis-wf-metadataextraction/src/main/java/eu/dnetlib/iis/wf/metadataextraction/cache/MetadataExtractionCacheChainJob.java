package eu.dnetlib.iis.wf.metadataextraction.cache;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.cache.CacheMetadataManagingProcess;
import eu.dnetlib.iis.common.cache.CacheStorageUtils;
import eu.dnetlib.iis.common.cache.CacheStorageUtils.CacheRecordType;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.lock.LockManager;
import eu.dnetlib.iis.common.lock.LockManagerUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.importer.schemas.DocumentContent;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.wf.metadataextraction.MetadataExtractorJob;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * Spark port of the Oozie metadata extraction cache workflows:
 * <ul>
 *   <li>{@code eu/dnetlib/iis/wf/metadataextraction/cache/chain/oozie_app} (cache chain)</li>
 *   <li>{@code eu/dnetlib/iis/wf/metadataextraction/cache/create/oozie_app} (cache create)</li>
 *   <li>{@code eu/dnetlib/iis/wf/metadataextraction/cache/update/oozie_app} (cache update)</li>
 * </ul>
 *
 * <p>This is the "simplification" approach: the cache hierarchy (which in Oozie was spread over a
 * cumbersome set of conditional transitions plus ZooKeeper/HDFS lock and cache id management
 * utility actions) is offloaded into the Spark job, following the pattern already established by
 * {@code CachedWebCrawlerJob} / {@code CacheStorageUtils}. The surrounding reusable workflows
 * ({@code identify_by_checksum}, {@code cache/builder}, {@code importer/content_url/chain}) remain
 * Airflow DAGs.</p>
 *
 * <p>The input is a {@link DocumentContent} datastore whose ids are content checksums (result of
 * the checksum preprocessing transformation, so cache entries are identified by content checksum
 * instead of the OA identifier). The job:</p>
 * <ol>
 *   <li>returns empty meta/fault datastores when the input is empty;</li>
 *   <li>reads the existing cache id and reconstructs the cached meta/fault records
 *       (cache create branch when no cache exists yet);</li>
 *   <li>skips content already present in the cache (based on cached meta ids, which covers both
 *       successfully extracted and previously faulted documents because the latter are persisted
 *       with empty marker records);</li>
 *   <li>runs {@link MetadataExtractorJob#extract} over the outstanding content
 *       (cache update branch);</li>
 *   <li>merges the newly produced records into the cache under a new cache id guarded by the lock
 *       manager;</li>
 *   <li>writes the requested output: either the union of the records returned from cache and the
 *       newly extracted ones ({@code -returnAlreadyExtractedMeta=true}, i.e. the
 *       {@code identify_by_checksum} chain) or only the newly extracted records
 *       ({@code -returnAlreadyExtractedMeta=false}, i.e. the {@code cache/builder} update which
 *       does not need to return anything).</li>
 * </ol>
 *
 * <p>Cached faults are never propagated to the output; only newly produced faults are written,
 * exactly as in the original workflows.</p>
 */
public class MetadataExtractionCacheChainJob {

    private static final Logger log = Logger.getLogger(MetadataExtractionCacheChainJob.class);

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    private MetadataExtractionCacheChainJob() {
    }

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws Exception {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            Configuration hadoopConf = sc.hadoopConfiguration();
            Path cacheRootDir = new Path(params.cacheRootDir);
            CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();
            LockManager lockManager = LockManagerUtils.instantiateLockManager(
                    params.lockManagerFactoryClassName, hadoopConf);

            JavaRDD<DocumentContent> input = avroLoader.loadJavaRDD(sc, params.input, DocumentContent.class);
            input.cache();

            HdfsUtils.remove(hadoopConf, params.outputRoot);

            if (input.isEmpty()) {
                log.info("input is empty, generating empty meta and fault datastores without touching the cache");
                MetadataExtractorJob.ExtractionResult empty = MetadataExtractorJob.emptyResult(sc);
                storeOutput(sc, params, empty.meta, empty.fault);
                storeReports(sc, params, empty.reports, 0L);
                return;
            }

            String existingCacheId = cacheManager.getExistingCacheId(hadoopConf, cacheRootDir);
            log.info("existing cache id: " + existingCacheId);

            JavaRDD<ExtractedDocumentMetadata> cachedMeta = CacheStorageUtils.getRddOrEmpty(sc, avroLoader,
                    cacheRootDir, existingCacheId, CacheRecordType.data, ExtractedDocumentMetadata.class);
            JavaRDD<Fault> cachedFault = CacheStorageUtils.getRddOrEmpty(sc, avroLoader,
                    cacheRootDir, existingCacheId, CacheRecordType.fault, Fault.class);

            JavaPairRDD<CharSequence, Boolean> inputIds = input
                    .map(DocumentContent::getId)
                    .distinct()
                    .mapToPair(id -> new Tuple2<>(id, Boolean.TRUE));
            JavaPairRDD<CharSequence, Boolean> cachedMetaIds = cachedMeta
                    .map(ExtractedDocumentMetadata::getId)
                    .distinct()
                    .mapToPair(id -> new Tuple2<>(id, Boolean.TRUE));

            // skip already extracted content (mirrors the skip_extracted / skip_extracted_without_meta
            // PIG transformers: an id is skipped when it has ANY cached meta record, including the
            // empty marker records written for faulted documents)
            JavaRDD<DocumentContent> toBeProcessed = input
                    .mapToPair(content -> new Tuple2<>(content.getId(), content))
                    .leftOuterJoin(cachedMetaIds)
                    .filter(pair -> !pair._2._2.isPresent())
                    .map(pair -> pair._2._1);
            toBeProcessed.cache();

            // records already processed, returned from cache (empty marker records are filtered out)
            JavaRDD<ExtractedDocumentMetadata> returnedMeta = params.returnAlreadyExtractedMeta
                    ? cachedMeta
                            .filter(meta -> meta.getPublicationTypeName() == null
                                    || !MetadataExtractorJob.EMPTY_META.equals(
                                            meta.getPublicationTypeName().toString()))
                            .mapToPair(meta -> new Tuple2<>(meta.getId(), meta))
                            .join(inputIds)
                            .map(pair -> pair._2._1)
                    : sc.<ExtractedDocumentMetadata>emptyRDD();

            MetadataExtractorJob.ExtractionResult extraction;
            if (params.activeMetadataExtraction && !toBeProcessed.isEmpty()) {
                extraction = MetadataExtractorJob.extract(sc, toBeProcessed, params.toExtractionParams());
            } else {
                log.info("skipping metadata extraction: activeMetadataExtraction="
                        + params.activeMetadataExtraction + ", toBeProcessed empty=" + toBeProcessed.isEmpty());
                extraction = MetadataExtractorJob.emptyResult(sc);
            }

            JavaRDD<ExtractedDocumentMetadata> newMeta = extraction.meta;
            JavaRDD<Fault> newFault = extraction.fault;

            // cache update/create guarded by the lock manager
            if (params.activeMetadataExtraction && (extraction.metaCount > 0 || extraction.faultCount > 0)) {
                log.info("storing new cache entry: meta=" + extraction.metaCount + ", fault=" + extraction.faultCount);
                CacheStorageUtils.storeInCache(avroSaver, ExtractedDocumentMetadata.SCHEMA$,
                        cachedMeta.union(newMeta), cachedFault.union(newFault), cacheRootDir, lockManager,
                        cacheManager, hadoopConf, params.numberOfEmittedFilesInCache);
            } else {
                log.info("no new records to be stored in cache");
            }

            JavaRDD<ExtractedDocumentMetadata> mergedMeta = params.returnAlreadyExtractedMeta
                    ? returnedMeta.union(newMeta)
                    : newMeta;

            storeOutput(sc, params, mergedMeta, newFault);
            storeReports(sc, params, extraction.reports, returnedMeta.count());
        }
    }

    //------------------------ PRIVATE --------------------------

    private static void storeOutput(JavaSparkContext sc, Params params,
            JavaRDD<ExtractedDocumentMetadata> meta, JavaRDD<Fault> fault) {
        avroSaver.saveJavaRDD(meta.repartition(params.numberOfEmittedFiles), ExtractedDocumentMetadata.SCHEMA$,
                params.outputRoot + "/" + params.outputNameMeta);
        avroSaver.saveJavaRDD(fault.repartition(params.numberOfEmittedFiles), Fault.SCHEMA$,
                params.outputRoot + "/" + params.outputNameFault);
    }

    private static void storeReports(JavaSparkContext sc, Params params,
            JavaRDD<ReportEntry> extractionReports, long fromCacheCount) {
        if (StringUtils.isBlank(params.outputReportRootPath)) {
            return;
        }
        // extraction reports (import.metadataExtraction.processed.*)
        avroSaver.saveJavaRDD(extractionReports.repartition(1), ReportEntry.SCHEMA$,
                params.outputReportRootPath + "/" + params.extractionReportRelativePath);

        // cache reports (import.metadataExtraction.fromCache.docMetadata)
        ReportEntry fromCacheEntry = ReportEntryFactory.createCounterReportEntry(
                params.reportPropertiesPrefix + ".fromCache.docMetadata", fromCacheCount);
        avroSaver.saveJavaRDD(sc.parallelize(Lists.newArrayList(fromCacheEntry)).repartition(1),
                ReportEntry.SCHEMA$, params.outputReportRootPath + "/" + params.cacheReportRelativePath);
    }

    //------------------------ PARAMETERS --------------------------

    @Parameters(separators = "=")
    public static class Params implements Serializable {

        private static final long serialVersionUID = 1L;

        @Parameter(names = "-input", required = true)
        public String input;

        @Parameter(names = "-outputRoot", required = true)
        public String outputRoot;

        @Parameter(names = "-outputNameMeta")
        public String outputNameMeta = "meta";

        @Parameter(names = "-outputNameFault")
        public String outputNameFault = "fault";

        @Parameter(names = "-numberOfEmittedFiles")
        public int numberOfEmittedFiles = 1;

        // --- cache settings ---
        @Parameter(names = "-cacheRootDir", required = true)
        public String cacheRootDir;

        @Parameter(names = "-lockManagerFactoryClassName", required = true)
        public String lockManagerFactoryClassName;

        @Parameter(names = "-numberOfEmittedFilesInCache")
        public int numberOfEmittedFilesInCache = 1000;

        @Parameter(names = "-activeMetadataExtraction")
        public boolean activeMetadataExtraction = true;

        @Parameter(names = "-returnAlreadyExtractedMeta")
        public boolean returnAlreadyExtractedMeta = true;

        // --- reporting ---
        @Parameter(names = "-outputReportRootPath")
        public String outputReportRootPath;

        @Parameter(names = "-extractionReportRelativePath")
        public String extractionReportRelativePath = "import_metadataextraction";

        @Parameter(names = "-cacheReportRelativePath")
        public String cacheReportRelativePath = "import_metadataextraction_cache";

        @Parameter(names = "-reportPropertiesPrefix")
        public String reportPropertiesPrefix = "import.metadataExtraction";

        // --- metadata extraction parameters (delegated to MetadataExtractorJob) ---
        @Parameter(names = "-excludedIds")
        public String excludedIds = MetadataExtractorJob.UNDEFINED;

        @Parameter(names = "-interruptProcessingTimeThresholdSecs")
        public Integer interruptProcessingTimeThresholdSecs;

        @Parameter(names = "-logFaultProcessingTimeThresholdSecs")
        public Integer logFaultProcessingTimeThresholdSecs;

        @Parameter(names = "-grobidServerUrl")
        public String grobidServerUrl = MetadataExtractorJob.UNDEFINED;

        @Parameter(names = "-grobidServerVersion")
        public String grobidServerVersion = MetadataExtractorJob.UNDEFINED;

        @Parameter(names = "-grobidServerConnectionTimeout")
        public int grobidServerConnectionTimeout = 600000;

        @Parameter(names = "-grobidServerReadTimeout")
        public int grobidServerReadTimeout = 600000;

        @Parameter(names = "-grobidServerThrottleSleepTime")
        public long grobidServerThrottleSleepTime = 60000;

        @Parameter(names = "-grobidServerMaxRetriesCount")
        public int grobidServerMaxRetriesCount = 10;

        /**
         * Builds the parameter set of the underlying extraction job.
         */
        MetadataExtractorJob.Params toExtractionParams() {
            MetadataExtractorJob.Params extractionParams = new MetadataExtractorJob.Params();
            extractionParams.input = input;
            extractionParams.outputRoot = outputRoot;
            extractionParams.excludedIds = excludedIds;
            extractionParams.interruptProcessingTimeThresholdSecs = interruptProcessingTimeThresholdSecs;
            extractionParams.logFaultProcessingTimeThresholdSecs = logFaultProcessingTimeThresholdSecs;
            extractionParams.grobidServerUrl = grobidServerUrl;
            extractionParams.grobidServerVersion = grobidServerVersion;
            extractionParams.grobidServerConnectionTimeout = grobidServerConnectionTimeout;
            extractionParams.grobidServerReadTimeout = grobidServerReadTimeout;
            extractionParams.grobidServerThrottleSleepTime = grobidServerThrottleSleepTime;
            extractionParams.grobidServerMaxRetriesCount = grobidServerMaxRetriesCount;
            return extractionParams;
        }
    }
}
