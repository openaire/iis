package eu.dnetlib.iis.wf.citationmatching.input;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
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
    
    public static void main(String[] args) throws InterruptedException {
        
        CitationMatchingInputTransformerJobParameters params = new CitationMatchingInputTransformerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> inputDocuments = avroLoader.loadJavaRDD(sc, params.inputMetadata, ExtractedDocumentMetadataMergedWithOriginal.class);
            
            JavaPairRDD<String, DocumentMetadata> documents = inputDocuments.mapToPair(
                    document -> new Tuple2<>(document.getId().toString(), documentToCitationDocumentConverter.convert(document)));
            
            avroSaver.saveJavaRDD(documents.values(), DocumentMetadata.SCHEMA$, params.output);
            
        }
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    @Parameters(separators = "=")
    private static class CitationMatchingInputTransformerJobParameters {
        
        @Parameter(names = "-inputMetadata", required = true)
        private String inputMetadata;
        
        @Parameter(names = "-output", required = true)
        private String output;
        
    }
}
