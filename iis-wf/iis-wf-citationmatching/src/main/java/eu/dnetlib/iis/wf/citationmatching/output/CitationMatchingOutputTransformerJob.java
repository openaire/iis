package eu.dnetlib.iis.wf.citationmatching.output;

import java.time.Year;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.citationmatching.schemas.Citation;
import eu.dnetlib.iis.common.cache.CacheMetadataManagingProcess;
import eu.dnetlib.iis.common.cache.CacheStorageUtils;
import eu.dnetlib.iis.common.cache.CacheStorageUtils.CacheRecordType;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.lock.LockManager;
import eu.dnetlib.iis.common.lock.LockManagerUtils;
import eu.dnetlib.iis.export.schemas.Citations;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * Output transformer job for citation matching.
 * It converts avro datastore from {@link eu.dnetlib.iis.citationmatching.schemas.Citation}
 * to {@link eu.dnetlib.iis.common.citations.schemas.Citation} format
 * 
 * @author madryk
 *
 */
public class CitationMatchingOutputTransformerJob {

    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    private static CitationToCommonCitationConverter citationToCommonCitationConverter = new CitationToCommonCitationConverter();
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws Exception {
        
        CitationMatchingOutputTransformerJobParameters params = new CitationMatchingOutputTransformerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            Configuration hadoopConf = sc.hadoopConfiguration();
            
        	HdfsUtils.remove(hadoopConf, params.output);
        	
        	int numberOfEmittedFiles = params.numberOfEmittedFiles;
            
            LockManager lockManager = LockManagerUtils.instantiateLockManager(params.lockManagerFactoryClassName,
                    hadoopConf);
        	
            JavaRDD<Citation> inputMatchedCitations = avroLoader.loadJavaRDD(sc, params.inputMatchedCitations, Citation.class);
            JavaRDD<eu.dnetlib.iis.common.citations.schemas.Citation> transformedMatchedCitations = 
                    inputMatchedCitations.map(inputCitation -> citationToCommonCitationConverter.convert(inputCitation));
            
            JavaPairRDD<CharSequence, CitationEntry> transformedMatchedCitationsPair = transformedMatchedCitations.mapToPair(
                    x -> new Tuple2<CharSequence, CitationEntry>(x.getSourceDocumentId(), x.getEntry())); 
            
            final Path cacheRootDir = new Path(params.cacheRootDir);
            CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess() ;
            String existingCacheId = cacheManager.getExistingCacheId(hadoopConf, cacheRootDir);
            
            // reading all cached entries
            JavaRDD<Citations> allCachedCitations = CacheStorageUtils.getRddOrEmpty(sc, avroLoader, cacheRootDir,
                    existingCacheId, CacheRecordType.data, Citations.class);
            allCachedCitations.cache();
            
            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> inputDocuments = avroLoader.loadJavaRDD(sc,
                    params.inputMetadata, ExtractedDocumentMetadataMergedWithOriginal.class);
            inputDocuments.cache();

            // caching only a subset of matched citations coming from a paper published earlier that X years ago.
            int publicationYearThreadshold = Year.now().getValue() - params.cacheOlderThanXYears;
            JavaPairRDD<CharSequence, Boolean> inputDocumentIdsAll = inputDocuments.mapToPair(x -> new Tuple2<CharSequence, Boolean>(x.getId(), true));
            JavaPairRDD<CharSequence, Boolean> inputDocumentIdsEligibleForCaching = inputDocuments.filter(x -> x.getYear() < publicationYearThreadshold).mapToPair(
                    x -> new Tuple2<CharSequence, Boolean>(x.getId(), true));
            // TODO what to do with the ones not having a publication year specified: include in cache or rather discard for further rerun?
            // currently they are not stored in cache, so they will be rerun
            // TODO check how many of records having text do not have publication year specified - this will reveal how many will be constantly rerun
            
            // subset of matched citations which are eligible for caching 
            JavaRDD<Citations> inputMatchedCitationsEligibleForCaching = transformedMatchedCitationsPair.join(inputDocumentIdsEligibleForCaching)
                    .mapToPair(x -> new Tuple2<CharSequence, CitationEntry>(x._1, x._2._1))
                    .groupByKey().map(x -> convertCitations(x._1, x._2));
            
            // unioning with already cached entries and storing as a new cache entry
            // notice: currently we do not care about faults, there are no faults generated in this process
            CacheStorageUtils.storeInCache(avroSaver, Citations.SCHEMA$,
                    allCachedCitations.union(inputMatchedCitationsEligibleForCaching), sc.emptyRDD(), cacheRootDir,
                    lockManager, cacheManager, hadoopConf, numberOfEmittedFiles);
            
            // removing citations from cache which were not presented at input of citation matching in this run (inner join with citation matching input)
            JavaRDD<eu.dnetlib.iis.common.citations.schemas.Citation> cachedCitationsToBeReturned = allCachedCitations
                    .mapToPair(x -> new Tuple2<CharSequence, Citations>(x.getDocumentId(), x)).join(inputDocumentIdsAll)
                    .flatMap(x -> convertCitations(x._2._1));
            
            avroSaver.saveJavaRDD(cachedCitationsToBeReturned.union(transformedMatchedCitations), eu.dnetlib.iis.common.citations.schemas.Citation.SCHEMA$, params.output);
        }
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    @Parameters(separators = "=")
    private static class CitationMatchingOutputTransformerJobParameters {
        
        @Parameter(names = "-inputMetadata", required = true)
        private String inputMetadata;
        
        @Parameter(names = "-inputMatchedCitations", required = true)
        private String inputMatchedCitations;
        
        @Parameter(names = "-cacheRootDir", required = true)
        private String cacheRootDir;
        
        @Parameter(names = "-cacheOlderThanXYears", required = true)
        private Integer cacheOlderThanXYears;
        
        @Parameter(names = "-output", required = true)
        private String output;
        
        @Parameter(names = "-numberOfEmittedFiles", required = true)
        private int numberOfEmittedFiles;
        
        @Parameter(names = "-lockManagerFactoryClassName", required = true)
        private String lockManagerFactoryClassName;
        
    }
    
    /**
     * Converts individual citation entries into a complex {@link Citations} object.
     * @param documentId document id
     * @param citationEntries citation entries
     */
    private static Citations convertCitations(CharSequence documentId, Iterable<CitationEntry> citationEntries) {
        List<CitationEntry> citations = new ArrayList<>();
        for (CitationEntry entry : citationEntries) {
            citations.add(entry);
        }
        Citations.Builder citationsBuilder = Citations.newBuilder();
        citationsBuilder.setDocumentId(documentId);
        citationsBuilder.setCitations(citations);
        return citationsBuilder.build();
    }
    
    /**
     * Converts {@link Citations} object into an {@link Iterator} over the individual {@link eu.dnetlib.iis.common.citations.schemas.Citation} records.
     */
    private static Iterator<eu.dnetlib.iis.common.citations.schemas.Citation> convertCitations(
            Citations citationsObject) {
        List<eu.dnetlib.iis.common.citations.schemas.Citation> citationsList = new ArrayList<>(
                citationsObject.getCitations().size());
        for (CitationEntry entry : citationsObject.getCitations()) {
            eu.dnetlib.iis.common.citations.schemas.Citation.Builder citationBuilder = eu.dnetlib.iis.common.citations.schemas.Citation
                    .newBuilder();
            citationBuilder.setSourceDocumentId(citationsObject.getDocumentId());
            citationBuilder.setEntry(entry);
            citationsList.add(citationBuilder.build());
        }
        return citationsList.iterator();
    }
    
}
