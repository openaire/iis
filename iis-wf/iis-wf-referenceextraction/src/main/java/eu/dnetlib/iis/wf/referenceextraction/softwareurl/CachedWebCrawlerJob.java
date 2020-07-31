package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.cache.CacheMetadataManagingProcess;
import eu.dnetlib.iis.common.cache.CacheStorageUtils;
import eu.dnetlib.iis.common.cache.CacheStorageUtils.CacheRecordType;
import eu.dnetlib.iis.common.cache.CacheStorageUtils.CachedStorageJobParameters;
import eu.dnetlib.iis.common.cache.CacheStorageUtils.OutputPaths;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.lock.LockManager;
import eu.dnetlib.iis.common.lock.LockManagerUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrl;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrlWithSource;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * Retrieves HTML pages pointed by softwareURL field from {@link DocumentToSoftwareUrl}.
 * For each {@link DocumentToSoftwareUrl} input record builds {@link DocumentToSoftwareUrlWithSource} if the page was available.
 * Stores results in cache for further usage.
 * @author mhorst
 *
 */
public class CachedWebCrawlerJob {
    
    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    public static final Logger log = Logger.getLogger(CachedWebCrawlerJob.class);
    
    private static final String COUNTER_PROCESSED_TOTAL = "processing.referenceExtraction.softwareUrl.webcrawl.processed.total";
    
    private static final String COUNTER_PROCESSED_FAULT = "processing.referenceExtraction.softwareUrl.webcrawl.processed.fault";
    
    private static final String COUNTER_FROMCACHE_TOTAL = "processing.referenceExtraction.softwareUrl.webcrawl.fromCache.total";
    
    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws Exception {
        WebCrawlerJobParameters params = new WebCrawlerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        final String cacheRootDir = CacheStorageUtils.normalizePath(params.getCacheRootDir());
        
        ContentRetrieverContext contentRetrieverContext = new ContentRetrieverContext(params.contentRetrieverClassName, 
                params.connectionTimeout, params.readTimeout, params.maxPageContentLength, params.numberOfEmittedFiles);
        
        CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();
        
        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.getOutputPath());
            HdfsUtils.remove(sc.hadoopConfiguration(), params.getOutputFaultPath());
            HdfsUtils.remove(sc.hadoopConfiguration(), params.getOutputReportPath());

            LockManager lockManager = LockManagerUtils.instantiateLockManager(params.getLockManagerFactoryClassName(),
                    sc.hadoopConfiguration());
            
            OutputPaths outputPaths = new OutputPaths(params);
            
            JavaRDD<DocumentToSoftwareUrl> documentToSoftwareUrl = avroLoader.loadJavaRDD(sc, params.inputPath,
                    DocumentToSoftwareUrl.class);
            
            if (documentToSoftwareUrl.isEmpty()) {
                storeInOutput(sc.emptyRDD(), sc.emptyRDD(),
                        generateReportEntries(sc, sc.emptyRDD(), sc.emptyRDD(), sc.emptyRDD()), 
                        outputPaths, contentRetrieverContext.getNumberOfEmittedFiles());
                return;
            }
            
            String existingCacheId = cacheManager.getExistingCacheId(sc.hadoopConfiguration(), cacheRootDir);
            
            // checking whether cache is empty
            if (CacheMetadataManagingProcess.UNDEFINED.equals(existingCacheId)) {
                
                createCache(documentToSoftwareUrl, cacheRootDir, 
                        contentRetrieverContext, lockManager, sc, cacheManager, outputPaths);
            } else {
                updateCache(documentToSoftwareUrl, cacheRootDir, existingCacheId, 
                        contentRetrieverContext, lockManager, sc, cacheManager, outputPaths);
            }
        }
    }
    
    //------------------------ PRIVATE --------------------------

    private static void createCache(JavaRDD<DocumentToSoftwareUrl> documentToSoftwareUrl,
            String cacheRootDir, ContentRetrieverContext contentRetrieverContext, 
            LockManager lockManager, JavaSparkContext sc, CacheMetadataManagingProcess cacheManager,
            OutputPaths outputPaths) throws Exception {
        
        Configuration hadoopConf = sc.hadoopConfiguration();
        
        Tuple2<JavaRDD<DocumentText>, JavaRDD<Fault>> returnedFromWebcrawlTuple = WebCrawlerUtils.obtainSources(documentToSoftwareUrl, contentRetrieverContext);
        
        if (!returnedFromWebcrawlTuple._1.isEmpty()) {
            // will be written in two output datastores: cache and result
            returnedFromWebcrawlTuple._1.cache();
            returnedFromWebcrawlTuple._2.cache();
            // storing new cache entry
            CacheStorageUtils.storeInCache(avroSaver, returnedFromWebcrawlTuple._1, 
                    returnedFromWebcrawlTuple._2, 
                    cacheRootDir, lockManager, cacheManager, hadoopConf, contentRetrieverContext.getNumberOfEmittedFiles());
        }
        
        // store final results
        JavaRDD<DocumentToSoftwareUrlWithSource> entitiesToBeStored = produceEntitiesToBeStored(documentToSoftwareUrl, returnedFromWebcrawlTuple._1);
        entitiesToBeStored.cache();
        JavaRDD<Fault> faultsToBeStored = produceFaultToBeStored(documentToSoftwareUrl, returnedFromWebcrawlTuple._2);
        faultsToBeStored.cache();
        
        storeInOutput(entitiesToBeStored, faultsToBeStored,
                generateReportEntries(sc, sc.emptyRDD(), entitiesToBeStored, faultsToBeStored),
                outputPaths, contentRetrieverContext.getNumberOfEmittedFiles());
    }
    
    private static void updateCache(JavaRDD<DocumentToSoftwareUrl> documentToSoftwareUrl,
            String cacheRootDir, String existingCacheId, ContentRetrieverContext contentRetrieverContext, 
            LockManager lockManager, JavaSparkContext sc, CacheMetadataManagingProcess cacheManager,
            OutputPaths outputPaths) throws Exception {
        
        Configuration hadoopConf = sc.hadoopConfiguration();
        
        // skipping already extracted
        JavaRDD<DocumentText> cachedSources = avroLoader.loadJavaRDD(sc, 
                CacheStorageUtils.getCacheLocation(cacheRootDir, existingCacheId, CacheRecordType.text), DocumentText.class);
        // will be written in new cache version and output
        cachedSources.cache();
        
        JavaRDD<Fault> cachedFaults = avroLoader.loadJavaRDD(sc, 
                CacheStorageUtils.getCacheLocation(cacheRootDir, existingCacheId, CacheRecordType.fault), Fault.class);
        
        JavaPairRDD<CharSequence, CharSequence> cacheByUrl = cachedSources.mapToPair(x -> new Tuple2<>(x.getId(), x.getText()));
        JavaPairRDD<CharSequence, DocumentToSoftwareUrl> inputByUrl = documentToSoftwareUrl.mapToPair(x -> new Tuple2<>(x.getSoftwareUrl(), x));
        JavaPairRDD<CharSequence, Tuple2<DocumentToSoftwareUrl, Optional<CharSequence>>> inputJoinedWithCache = inputByUrl.leftOuterJoin(cacheByUrl);

        JavaRDD<DocumentToSoftwareUrl> toBeProcessed = inputJoinedWithCache.filter(x -> !x._2._2.isPresent()).values().map(x -> x._1);
        JavaRDD<DocumentToSoftwareUrlWithSource> entitiesReturnedFromCache = inputJoinedWithCache.filter(x -> x._2._2.isPresent()).values().map(x -> attachSource(x._1, x._2.get()));
        entitiesReturnedFromCache.cache();
        
        Tuple2<JavaRDD<DocumentText>, JavaRDD<Fault>>  returnedFromWebcrawlTuple = WebCrawlerUtils.obtainSources(toBeProcessed, contentRetrieverContext);

        JavaRDD<DocumentToSoftwareUrlWithSource> webcrawledEntities;
        JavaRDD<DocumentToSoftwareUrlWithSource> entitiesToBeWritten;
        
        if (!returnedFromWebcrawlTuple._1.isEmpty()) {
            // will be written in two output datastores: cache and result
            returnedFromWebcrawlTuple._1.cache();
            returnedFromWebcrawlTuple._2.cache();
            
            // storing new cache entry
            CacheStorageUtils.storeInCache(avroSaver, cachedSources.union(returnedFromWebcrawlTuple._1), 
                    cachedFaults.union(returnedFromWebcrawlTuple._2), 
                    cacheRootDir, lockManager, cacheManager, hadoopConf, contentRetrieverContext.getNumberOfEmittedFiles());
            
            // merging final results
            webcrawledEntities = produceEntitiesToBeStored(toBeProcessed, returnedFromWebcrawlTuple._1);
            webcrawledEntities.cache();
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
                outputPaths, contentRetrieverContext.getNumberOfEmittedFiles());
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
        avroSaver.saveJavaRDD(entities.coalesce(numberOfEmittedFiles), DocumentToSoftwareUrlWithSource.SCHEMA$, outputPaths.getResult());
        avroSaver.saveJavaRDD(faults.coalesce(numberOfEmittedFiles), Fault.SCHEMA$, outputPaths.getFault());
        avroSaver.saveJavaRDD(reports.coalesce(numberOfEmittedFiles), ReportEntry.SCHEMA$, outputPaths.getReport());
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
        
        @Parameter(names = "-contentRetrieverClassName", required = true)
        private String contentRetrieverClassName;

        @Parameter(names = "-connectionTimeout", required = true)
        private int connectionTimeout;
        
        @Parameter(names = "-readTimeout", required = true)
        private int readTimeout;
        
        @Parameter(names = "-maxPageContentLength", required = true)
        private int maxPageContentLength;
        
        @Parameter(names = "-numberOfEmittedFiles", required = true)
        private int numberOfEmittedFiles;

        
    }
}
