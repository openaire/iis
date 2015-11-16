package eu.dnetlib.iis.workflows.transformers.spark.citationmatching.direct;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import eu.dnetlib.iis.common.spark.avro.SparkAvroLoader;
import eu.dnetlib.iis.common.spark.avro.SparkAvroSaver;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;

public class SparkCitationMatchingDirectTransformer {

    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        SparkCMDirectTransformerParameters params = new SparkCMDirectTransformerParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        
        
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> input = SparkAvroLoader.loadJavaRDD(sc, params.inputAvroPath, ExtractedDocumentMetadataMergedWithOriginal.class);
            
            
            DocumentToDirectCitationMetadataConverter converter = new DocumentToDirectCitationMetadataConverter();
            JavaRDD<DocumentMetadata> output = input.map(metadata -> converter.convert(metadata));
            
            
            SparkAvroSaver.saveJavaRDD(output, DocumentMetadata.SCHEMA$, params.outputAvroPath);
        }
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    
    @Parameters(separators = "=")
    private static class SparkCMDirectTransformerParameters {
        
        @Parameter(names = "-inputAvroPath", required = true)
        private String inputAvroPath;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;
        
        
    }
}
