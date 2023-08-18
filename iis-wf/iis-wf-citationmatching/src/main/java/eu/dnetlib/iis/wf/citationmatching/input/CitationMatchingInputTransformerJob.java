package eu.dnetlib.iis.wf.citationmatching.input;

import java.io.IOException;
import java.util.Collections;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.citations.schemas.Citation;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
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
        	
        	HdfsUtils.remove(sc.hadoopConfiguration(), params.output);
            
            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> inputDocuments = avroLoader.loadJavaRDD(sc, params.inputMetadata, ExtractedDocumentMetadataMergedWithOriginal.class);
            
            JavaRDD<DocumentMetadata> documents;
            
            if (StringUtils.isNotBlank(params.inputMatchedCitations) && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(params.inputMatchedCitations)) {
                //filtering mode
                JavaRDD<Citation> matchedCitations = avroLoader.loadJavaRDD(sc, params.inputMatchedCitations, Citation.class);
                
                JavaPairRDD<CharSequence, Iterable<Integer>> groupedCitations = matchedCitations
                        .mapToPair(cit -> new Tuple2<>(cit.getSourceDocumentId(), cit.getEntry().getPosition()))
                        .groupByKey();
                
                JavaPairRDD<CharSequence, ExtractedDocumentMetadataMergedWithOriginal> pairedDocuments = inputDocuments.mapToPair(doc -> new Tuple2<>(doc.getId(), doc));
                
                JavaPairRDD<CharSequence, Tuple2<ExtractedDocumentMetadataMergedWithOriginal, Optional<Iterable<Integer>>>> inputDocumentsJoinedWithMatchedCitations = pairedDocuments
                        .leftOuterJoin(groupedCitations);
                documents = inputDocumentsJoinedWithMatchedCitations
                        .map(x -> documentToCitationDocumentConverter.convert(x._2._1,
                                x._2._2.isPresent() ? Sets.newHashSet(x._2._2.get()) : Collections.emptySet()));

            } else {
                documents = inputDocuments.map(document -> documentToCitationDocumentConverter.convert(document, Collections.emptySet()));    
            }

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
        
        @Parameter(names = "-output", required = true)
        private String output;
        
    }
}
