package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import java.util.Map;

import eu.dnetlib.iis.common.fault.FaultUtils;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetriever;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetrieverResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.storage.StorageLevel;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.cache.CacheMetadataManagingProcess;
import eu.dnetlib.iis.common.cache.DocumentTextCacheStorageUtils;
import eu.dnetlib.iis.common.cache.DocumentTextCacheStorageUtils.CacheRecordType;
import eu.dnetlib.iis.common.cache.DocumentTextCacheStorageUtils.CachedStorageJobParameters;
import eu.dnetlib.iis.common.cache.DocumentTextCacheStorageUtils.OutputPaths;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.lock.LockManager;
import eu.dnetlib.iis.common.lock.LockManagerUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrl;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrlWithSource;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeUtils;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * Retrieves HTML pages pointed by softwareURL field from {@link DocumentToSoftwareUrl}.
 * For each {@link DocumentToSoftwareUrl} input record builds {@link DocumentToSoftwareUrlWithSource} if the page was available.
 * Stores results in cache for further usage.
 */
public class CachedWebCrawlerJob {
    
    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    private static final String COUNTER_PROCESSED_TOTAL = "processing.referenceExtraction.softwareUrl.webcrawl.processed.total";
    
    private static final String COUNTER_PROCESSED_FAULT = "processing.referenceExtraction.softwareUrl.webcrawl.processed.fault";
    
    private static final String COUNTER_FROMCACHE_TOTAL = "processing.referenceExtraction.softwareUrl.webcrawl.fromCache.total";
    
    private static final StorageLevel CACHE_STORAGE_DEFAULT_LEVEL = StorageLevel.DISK_ONLY();
    
    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws Exception {
        WebCrawlerJobParameters params = new WebCrawlerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            
            Configuration hadoopConf = sc.hadoopConfiguration();
            
            HdfsUtils.remove(hadoopConf, params.getOutputPath());
            HdfsUtils.remove(hadoopConf, params.getOutputFaultPath());
            HdfsUtils.remove(hadoopConf, params.getOutputReportPath());

            FacadeContentRetriever<String, String> contentRetriever = ServiceFacadeUtils
                    .instantiate(params.httpServiceFacadeFactoryClassName, params.httpServiceFacadeParams);

            int numberOfPartitionsForCrawling = params.numberOfPartitionsForCrawling;
            int numberOfEmittedFiles = params.numberOfEmittedFiles;
            
            LockManager lockManager = LockManagerUtils.instantiateLockManager(params.getLockManagerFactoryClassName(),
                    hadoopConf);
            
            JavaRDD<DocumentToSoftwareUrl> documentToSoftwareUrl = avroLoader.loadJavaRDD(sc, params.inputPath,
                    DocumentToSoftwareUrl.class);
            
            final Path cacheRootDir = new Path(params.getCacheRootDir());
            CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();
            String existingCacheId = cacheManager.getExistingCacheId(hadoopConf, cacheRootDir);
            
            // skipping already extracted
            JavaRDD<DocumentText> cachedSources = DocumentTextCacheStorageUtils.getRddOrEmpty(sc, avroLoader, cacheRootDir,
                    existingCacheId, CacheRecordType.text, DocumentText.class);
            
            JavaPairRDD<CharSequence, CharSequence> cacheByUrl = cachedSources.mapToPair(x -> new Tuple2<>(x.getId(), x.getText()));
            JavaPairRDD<CharSequence, DocumentToSoftwareUrl> inputByUrl = documentToSoftwareUrl.mapToPair(x -> new Tuple2<>(x.getSoftwareUrl(), x));
            JavaPairRDD<CharSequence, Tuple2<DocumentToSoftwareUrl, Optional<CharSequence>>> inputJoinedWithCache = inputByUrl.leftOuterJoin(cacheByUrl);

            JavaRDD<DocumentToSoftwareUrl> toBeProcessed = inputJoinedWithCache.filter(x -> !x._2._2.isPresent()).values().map(x -> x._1);
            JavaRDD<DocumentToSoftwareUrlWithSource> entitiesReturnedFromCache = inputJoinedWithCache.filter(x -> x._2._2.isPresent()).values().map(x -> attachSource(x._1, x._2.get()));
            entitiesReturnedFromCache.persist(CACHE_STORAGE_DEFAULT_LEVEL);

            JavaPairRDD<CharSequence, FacadeContentRetrieverResponse<String>> returnedFromRemoteService =
                    retrieveFromRemoteService(toBeProcessed, numberOfPartitionsForCrawling, contentRetriever);

            JavaRDD<DocumentText> retrievedSourcesToBeCached = mapContentRetrieverResponsesToDocumentTextForCache(
                    returnedFromRemoteService);
            JavaRDD<Fault> faultsToBeCached = mapContentRetrieverResponsesToFaultForCache(
                    returnedFromRemoteService);
            Tuple2<JavaRDD<DocumentText>, JavaRDD<Fault>> returnedFromWebcrawlTupleToBeCached = new Tuple2<>(
                    retrievedSourcesToBeCached, faultsToBeCached);
            if (!returnedFromWebcrawlTupleToBeCached._1.isEmpty()) {
                returnedFromWebcrawlTupleToBeCached._1.persist(CACHE_STORAGE_DEFAULT_LEVEL);
                returnedFromWebcrawlTupleToBeCached._2.persist(CACHE_STORAGE_DEFAULT_LEVEL);

                JavaRDD<Fault> cachedFaults = DocumentTextCacheStorageUtils.getRddOrEmpty(sc, avroLoader, cacheRootDir,
                        existingCacheId, CacheRecordType.fault, Fault.class);

                // storing new cache entry
                DocumentTextCacheStorageUtils.storeInCache(avroSaver, cachedSources.union(returnedFromWebcrawlTupleToBeCached._1),
                        cachedFaults.union(returnedFromWebcrawlTupleToBeCached._2),
                        cacheRootDir, lockManager, cacheManager, hadoopConf, numberOfEmittedFiles);
            }

            JavaRDD<DocumentText> retrievedSources  = mapContentRetrieverResponsesToDocumentTextForOutput(
                    returnedFromRemoteService);
            JavaRDD<Fault> faults = mapContentRetrieverResponsesToFaultForOutput(
                    returnedFromRemoteService);
            Tuple2<JavaRDD<DocumentText>, JavaRDD<Fault>> returnedFromWebcrawlTuple = new Tuple2<>(
                    retrievedSources, faults);

            JavaRDD<DocumentToSoftwareUrlWithSource> webcrawledEntities;
            JavaRDD<DocumentToSoftwareUrlWithSource> entitiesToBeWritten;
            if (!returnedFromWebcrawlTuple._1.isEmpty()){
                webcrawledEntities = produceEntitiesToBeStored(toBeProcessed, returnedFromWebcrawlTuple._1);
                webcrawledEntities.persist(CACHE_STORAGE_DEFAULT_LEVEL);
                entitiesToBeWritten = entitiesReturnedFromCache.union(webcrawledEntities);
            } else {
                webcrawledEntities = sc.emptyRDD();
                entitiesToBeWritten = entitiesReturnedFromCache;
            }
            
            // store final results
            JavaRDD<Fault> faultsToBeStored = produceFaultToBeStored(toBeProcessed, returnedFromWebcrawlTuple._2);
            faultsToBeStored.cache();
            
            storeInOutput(
                    entitiesToBeWritten, 
                    //notice: we do not propagate faults from cache, only new faults are written
                    faultsToBeStored, 
                    generateReportEntries(sc, entitiesReturnedFromCache, webcrawledEntities, faultsToBeStored),
                    new OutputPaths(params), numberOfEmittedFiles);
        }
    }
    
    //------------------------ PRIVATE --------------------------

    private static JavaPairRDD<CharSequence, FacadeContentRetrieverResponse<String>> retrieveFromRemoteService(JavaRDD<DocumentToSoftwareUrl> documentToSoftwareUrls,
                                                                                                               int numberOfPartitionsForCrawling,
                                                                                                               FacadeContentRetriever<String, String> contentRetriever) {
        JavaRDD<CharSequence> uniqueSoftwareUrls = documentToSoftwareUrls.map(DocumentToSoftwareUrl::getSoftwareUrl).distinct();
        return uniqueSoftwareUrls
                .repartition(numberOfPartitionsForCrawling)
                .mapToPair(url -> new Tuple2<>(url, contentRetriever.retrieveContent(url.toString())));
    }

    private static JavaRDD<DocumentText> mapContentRetrieverResponsesToDocumentTextForCache(
            JavaPairRDD<CharSequence, FacadeContentRetrieverResponse<String>> returnedFromRemoteService) {
        return returnedFromRemoteService
                .filter(e -> !e._2.getClass().equals(FacadeContentRetrieverResponse.TransientFailure.class))
                .map(e -> {
                    if (e._2.getClass().equals(FacadeContentRetrieverResponse.Success.class)) {
                        return DocumentText.newBuilder().setId(e._1).setText(e._2.getContent()).build();
                    }
                    return DocumentText.newBuilder().setId(e._1).setText("").build();
                });
    }

    private static JavaRDD<Fault> mapContentRetrieverResponsesToFaultForCache(
            JavaPairRDD<CharSequence, FacadeContentRetrieverResponse<String>> returnedFromRemoteService) {
        return returnedFromRemoteService
                .filter(e -> e._2.getClass().equals(FacadeContentRetrieverResponse.PersistentFailure.class))
                .map(e -> FaultUtils.exceptionToFault(e._1, e._2.getException(), null));
    }

    private static JavaRDD<DocumentText> mapContentRetrieverResponsesToDocumentTextForOutput(
            JavaPairRDD<CharSequence, FacadeContentRetrieverResponse<String>> returnedFromRemoteService) {
        return returnedFromRemoteService
                .map(e -> {
                    if (e._2.getClass().equals(FacadeContentRetrieverResponse.Success.class)) {
                        return DocumentText.newBuilder().setId(e._1).setText(e._2.getContent()).build();
                    }
                    return DocumentText.newBuilder().setId(e._1).setText("").build();
                });
    }

    private static JavaRDD<Fault> mapContentRetrieverResponsesToFaultForOutput(
            JavaPairRDD<CharSequence, FacadeContentRetrieverResponse<String>> returnedFromRemoteService) {
        return returnedFromRemoteService
                .filter(e -> FacadeContentRetrieverResponse.Failure.class.isAssignableFrom(e._2.getClass()))
                .map(e -> FaultUtils.exceptionToFault(e._1, e._2.getException(), null));
    }

    private static JavaRDD<ReportEntry> generateReportEntries(JavaSparkContext sparkContext, 
            JavaRDD<DocumentToSoftwareUrlWithSource> fromCacheEntities, JavaRDD<DocumentToSoftwareUrlWithSource> processedEntities, JavaRDD<Fault> processedFaults) {

        ReportEntry fromCacheEntitiesCounter = ReportEntryFactory.createCounterReportEntry(COUNTER_FROMCACHE_TOTAL, fromCacheEntities.count());
        ReportEntry processedEnttiesCounter = ReportEntryFactory.createCounterReportEntry(COUNTER_PROCESSED_TOTAL, processedEntities.count());
        ReportEntry processedFaultsCounter = ReportEntryFactory.createCounterReportEntry(COUNTER_PROCESSED_FAULT, processedFaults.count());
        
        return sparkContext.parallelize(Lists.newArrayList(fromCacheEntitiesCounter, processedEnttiesCounter, processedFaultsCounter));
    }
    
    /**
     * Matches webcrawler output identified by url with input identified by regular identifier.
     */
    private static JavaRDD<DocumentToSoftwareUrlWithSource> produceEntitiesToBeStored(JavaRDD<DocumentToSoftwareUrl> documentToSoftwareUrl, JavaRDD<DocumentText> documentText) {
        
        JavaPairRDD<CharSequence, DocumentToSoftwareUrl> documentToSoftwareUrlByUrl = documentToSoftwareUrl.mapToPair(e -> new Tuple2<>(e.getSoftwareUrl(), e));
        
        JavaPairRDD<CharSequence, CharSequence> documentSourceByUrl = documentText.mapToPair(e -> new Tuple2<>(e.getId(), e.getText()));
        JavaRDD<Tuple2<DocumentToSoftwareUrl, CharSequence>> softwareUrlJoinedWithSource = documentToSoftwareUrlByUrl
                .join(documentSourceByUrl).values();
        
        return softwareUrlJoinedWithSource.map(e -> buildDocumentToSoftwareUrlWithSource(e._1, e._2));
    }
    
    private static DocumentToSoftwareUrlWithSource buildDocumentToSoftwareUrlWithSource(DocumentToSoftwareUrl softwareUrl, CharSequence source) {
        DocumentToSoftwareUrlWithSource.Builder resultBuilder = DocumentToSoftwareUrlWithSource.newBuilder();
        resultBuilder.setDocumentId(softwareUrl.getDocumentId());
        resultBuilder.setSoftwareUrl(softwareUrl.getSoftwareUrl());
        resultBuilder.setRepositoryName(softwareUrl.getRepositoryName());
        resultBuilder.setSource(source);
        return resultBuilder.build();
    }
    
    /**
     * Matches webcrawler faults identified by url with input identified by regular identifier.
     */
    private static JavaRDD<Fault> produceFaultToBeStored(JavaRDD<DocumentToSoftwareUrl> documentToSoftwareUrl, JavaRDD<Fault> fault) {
        
        JavaPairRDD<CharSequence, CharSequence> documentIdByUrl = documentToSoftwareUrl.mapToPair(e -> new Tuple2<>(e.getSoftwareUrl(), e.getDocumentId()));
        
        JavaPairRDD<CharSequence, Fault> faultByUrl = fault.mapToPair(e -> new Tuple2<>(e.getInputObjectId(), e));
        JavaRDD<Tuple2<CharSequence, Fault>> idJoinedWithFault = documentIdByUrl.join(faultByUrl).values();
        
        return idJoinedWithFault.map(e -> updateFaultId(e._2, e._1));
    }
    
    private static Fault updateFaultId(Fault fault, CharSequence newId) {
        Fault.Builder clonedFaultBuilder = Fault.newBuilder(fault);
        clonedFaultBuilder.setInputObjectId(newId);
        return clonedFaultBuilder.build();
    }
    
    private static void storeInOutput(JavaRDD<DocumentToSoftwareUrlWithSource> entities, 
            JavaRDD<Fault> faults, JavaRDD<ReportEntry> reports, OutputPaths outputPaths, int numberOfEmittedFiles) {
        avroSaver.saveJavaRDD(entities.repartition(numberOfEmittedFiles), DocumentToSoftwareUrlWithSource.SCHEMA$, outputPaths.getResult());
        avroSaver.saveJavaRDD(faults.repartition(numberOfEmittedFiles), Fault.SCHEMA$, outputPaths.getFault());
        avroSaver.saveJavaRDD(reports.repartition(1), ReportEntry.SCHEMA$, outputPaths.getReport());
    }
    
    private static DocumentToSoftwareUrlWithSource attachSource(DocumentToSoftwareUrl record, CharSequence source) {
        DocumentToSoftwareUrlWithSource.Builder builder = DocumentToSoftwareUrlWithSource.newBuilder();
        builder.setDocumentId(record.getDocumentId());
        builder.setRepositoryName(record.getRepositoryName());
        builder.setSoftwareUrl(record.getSoftwareUrl());
        builder.setSource(source);
        return builder.build();
    }

    @Parameters(separators = "=")
    private static class WebCrawlerJobParameters extends CachedStorageJobParameters {
        
        @Parameter(names = "-inputPath", required = true)
        private String inputPath;
        
        @Parameter(names = "-numberOfEmittedFiles", required = true)
        private int numberOfEmittedFiles;
        
        @Parameter(names = "-numberOfPartitionsForCrawling", required = true)
        private int numberOfPartitionsForCrawling;

        @Parameter(names = "-httpServiceFacadeFactoryClassName", required = true)
        private String httpServiceFacadeFactoryClassName;

        @DynamicParameter(names = "-D", description = "dynamic parameters related to http service facade", required = false)
        private Map<String, String> httpServiceFacadeParams = Maps.newHashMap();
    }
}
