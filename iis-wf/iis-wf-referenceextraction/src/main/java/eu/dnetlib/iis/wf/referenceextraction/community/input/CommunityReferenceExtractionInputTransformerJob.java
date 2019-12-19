package eu.dnetlib.iis.wf.referenceextraction.community.input;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.importer.schemas.Concept;
import eu.dnetlib.iis.referenceextraction.community.schemas.Community;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 
 * @author mhorst
 *
 */
public class CommunityReferenceExtractionInputTransformerJob {

    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        CommunityExtractionInputTransformerJobParameters params = new CommunityExtractionInputTransformerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        final String acknowledgementParamName = params.acknowledgementParamName;
        
        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.output);
            
            JavaRDD<Concept> inputConcept = avroLoader.loadJavaRDD(sc, params.inputConcept, Concept.class);
            
            JavaRDD<Community> output = inputConcept.flatMap(x -> convert(x, acknowledgementParamName).iterator());

            avroSaver.saveJavaRDD(output, Community.SCHEMA$, params.output);
        }
        
    }

    //------------------------ PRIVATE --------------------------
    
    private static List<Community> convert(Concept concept, String acknowledgementParamName) {
        return concept.getParams().stream()
                .filter(x -> acknowledgementParamName.equals(x.getName().toString()))
                .map(x -> buildCommunity(concept.getId(), concept.getLabel(), StringUtils.defaultIfBlank(x.getValue(), "")))
                .collect(Collectors.toList());
    }
    
    private static Community buildCommunity(CharSequence id, CharSequence label, CharSequence acknowledgementStatement) {
        Community.Builder builder = Community.newBuilder();
        builder.setId(id);
        builder.setLabel(label);
        builder.setAcknowledgementStatement(acknowledgementStatement);
        return builder.build();
    }
    
    @Parameters(separators = "=")
    private static class CommunityExtractionInputTransformerJobParameters {
        
        @Parameter(names = "-inputConcept", required = true)
        private String inputConcept;
        
        @Parameter(names = "-acknowledgementParamName", required = true)
        private String acknowledgementParamName;
        
        @Parameter(names = "-output", required = true)
        private String output;
        
    }
}
