package eu.dnetlib.iis.workflows.citationmatching.direct;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;

public class CitationMatchingDirectInputTransformer {

    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        CMDirectInputTransformerParameters params = new CMDirectInputTransformerParameters();
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
    private static class CMDirectInputTransformerParameters {
        
        @Parameter(names = "-inputAvroPath", required = true)
        private String inputAvroPath;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;
        
        
    }
}
