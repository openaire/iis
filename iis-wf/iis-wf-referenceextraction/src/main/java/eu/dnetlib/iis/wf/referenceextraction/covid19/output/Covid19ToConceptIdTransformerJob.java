package eu.dnetlib.iis.wf.referenceextraction.covid19.output;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.referenceextraction.covid19.schemas.MatchedDocument;
import eu.dnetlib.iis.referenceextraction.researchinitiative.schemas.DocumentToConceptId;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * 
 * @author mhorst
 *
 */
public class Covid19ToConceptIdTransformerJob {
    
    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        Covid19ToConceptIdTransformerJobParameters params = new Covid19ToConceptIdTransformerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        String conceptId = params.predefinedConceptId;
        float confidenceLevel = Float.parseFloat(params.predefinedConfidenceLevel);
        
        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.output);
            
            JavaRDD<MatchedDocument> input = avroLoader.loadJavaRDD(sc, params.input, MatchedDocument.class);
            
            JavaRDD<DocumentToConceptId> output = input.map(x -> convertMetadata(x,conceptId, confidenceLevel));

            avroSaver.saveJavaRDD(output, DocumentToConceptId.SCHEMA$, params.output);
        }
        
    }

    //------------------------ PRIVATE --------------------------
    
    private static DocumentToConceptId convertMetadata(MatchedDocument input, String conceptId, float confidenceLevel) {
        DocumentToConceptId.Builder resultBuilder = DocumentToConceptId.newBuilder();
        resultBuilder.setDocumentId(input.getId());
        resultBuilder.setConceptId(conceptId);
        resultBuilder.setConfidenceLevel(confidenceLevel);
        return resultBuilder.build();
    }
    
    @Parameters(separators = "=")
    private static class Covid19ToConceptIdTransformerJobParameters {
        
        @Parameter(names = "-input", required = true)
        private String input;
        
        @Parameter(names = "-output", required = true)
        private String output;
        
        @Parameter(names = "-predefinedConceptId", required = true)
        private String predefinedConceptId;
        
        @Parameter(names = "-predefinedConfidenceLevel", required = true)
        private String predefinedConfidenceLevel;
        
    }
    
}
