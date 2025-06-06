package eu.dnetlib.iis.wf.citationmatching.input;

import java.io.IOException;
import java.util.Collections;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Sets;

import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.cache.CacheMetadataManagingProcess;
import eu.dnetlib.iis.common.cache.CacheStorageUtils;
import eu.dnetlib.iis.common.cache.CacheStorageUtils.CacheRecordType;
import eu.dnetlib.iis.common.citations.schemas.Citation;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.export.schemas.Citations;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * 
 * @author madryk
 *
 */
public class CitationMatchingInputTransformerJob {
    
    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    private static DocumentToCitationDocumentConverter documentToCitationDocumentConverter = new DocumentToCitationDocumentConverter();
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws InterruptedException, IOException {
        
        CitationMatchingInputTransformerJobParameters params = new CitationMatchingInputTransformerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            Configuration hadoopConf = sc.hadoopConfiguration();
        	
        	HdfsUtils.remove(hadoopConf, params.output);
            
            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> inputDocuments = avroLoader.loadJavaRDD(sc, params.inputMetadata, ExtractedDocumentMetadataMergedWithOriginal.class);
            
            JavaRDD<Citation> matchedCitations = (StringUtils.isNotBlank(params.inputMatchedCitations)
                    && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(params.inputMatchedCitations))
                            ? avroLoader.loadJavaRDD(sc, params.inputMatchedCitations, Citation.class)
                            : sc.emptyRDD();

            final Path cacheRootDir = new Path(params.cacheRootDir);
            CacheMetadataManagingProcess cacheManager = new CacheMetadataManagingProcess();
            String existingCacheId = cacheManager.getExistingCacheId(hadoopConf, cacheRootDir);
            
            // reading cached entries
            JavaRDD<Citations> cachedCitations = CacheStorageUtils.getRddOrEmpty(sc, avroLoader, cacheRootDir,
                    existingCacheId, CacheRecordType.data, Citations.class);

            //droping all bibrefs from input for all the matchable records present in cache
            JavaPairRDD<CharSequence, Boolean> groupedCachedCitations = cachedCitations.mapToPair(x -> new Tuple2<>(x.getDocumentId(), true));
            
            JavaPairRDD<CharSequence, Iterable<Integer>> groupedMatchedCitations = matchedCitations
                    .mapToPair(cit -> new Tuple2<>(cit.getSourceDocumentId(), cit.getEntry().getPosition()))
                    .groupByKey();
            
            JavaPairRDD<CharSequence, ExtractedDocumentMetadataMergedWithOriginal> pairedDocuments = inputDocuments.mapToPair(doc -> new Tuple2<>(doc.getId(), doc));
            
            JavaPairRDD<CharSequence, Tuple2<Tuple2<ExtractedDocumentMetadataMergedWithOriginal, Optional<Iterable<Integer>>>, Optional<Boolean>>> inputDocumentsJoinedWithMatchedCitations = pairedDocuments
                    .leftOuterJoin(groupedMatchedCitations).leftOuterJoin(groupedCachedCitations);
            
            JavaRDD<DocumentMetadata> documents = inputDocumentsJoinedWithMatchedCitations
                    .map(x -> {
                        ExtractedDocumentMetadataMergedWithOriginal inputDocumentMetadata = x._2._1._1;
                        Optional<Iterable<Integer>> alreadyMatchedReferencePositions = x._2._1._2;
                        Optional<Boolean> isDocumentAlreadyCached = x._2._2;

                        return documentToCitationDocumentConverter.convert(inputDocumentMetadata,
                                alreadyMatchedReferencePositions.isPresent()
                                        ? Sets.newHashSet(alreadyMatchedReferencePositions.get())
                                        : Collections.emptySet(),
                                isDocumentAlreadyCached.isPresent() ? isDocumentAlreadyCached.get() : false);
                    });

            avroSaver.saveJavaRDD(documents, DocumentMetadata.SCHEMA$, params.output);
        }
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    @Parameters(separators = "=")
    private static class CitationMatchingInputTransformerJobParameters {
        
        @Parameter(names = "-inputMetadata", required = true)
        private String inputMetadata;
        
        @Parameter(names = "-inputMatchedCitations", required = true)
        private String inputMatchedCitations;
        
        @Parameter(names = "-cacheRootDir", required = true)
        private String cacheRootDir;
        
        @Parameter(names = "-output", required = true)
        private String output;
        
    }

}
