package eu.dnetlib.iis.wf.citationmatching.output;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.citationmatching.schemas.Citation;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

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
    
    public static void main(String[] args) {
        
        CitationMatchingOutputTransformerJobParameters params = new CitationMatchingOutputTransformerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            JavaRDD<Citation> inputCitations = avroLoader.loadJavaRDD(sc, params.input, Citation.class);
            
            
            JavaRDD<eu.dnetlib.iis.common.citations.schemas.Citation> outputCitations = 
                    inputCitations.map(inputCitation -> citationToCommonCitationConverter.convert(inputCitation));
            
            
            avroSaver.saveJavaRDD(outputCitations, eu.dnetlib.iis.common.citations.schemas.Citation.SCHEMA$, params.output);
            
        }
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    @Parameters(separators = "=")
    private static class CitationMatchingOutputTransformerJobParameters {
        
        @Parameter(names = "-input", required = true)
        private String input;
        
        @Parameter(names = "-output", required = true)
        private String output;
        
    }
}
