package eu.dnetlib.iis.wf.referenceextraction.community.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.importer.schemas.Concept;
import eu.dnetlib.iis.importer.schemas.Param;
import eu.dnetlib.iis.referenceextraction.community.schemas.Community;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * 
 * @author mhorst
 *
 */
public class CommunityReferenceExtractionInputTransformerJob {
    

    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws InterruptedException, IOException {
        
        CommunityExtractionInputTransformerJobParameters params = new CommunityExtractionInputTransformerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        final String acknowledgementParamName = params.acknowledgementParamName; 
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            HdfsUtils.remove(sc.hadoopConfiguration(), params.output);
            
            JavaRDD<Concept> inputConcept = avroLoader.loadJavaRDD(sc, params.inputConcept, Concept.class);
            
            JavaRDD<Community> output = inputConcept.flatMap(x -> convert(x, acknowledgementParamName));

            avroSaver.saveJavaRDD(output, Community.SCHEMA$, params.output);
        }
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private static List<Community> convert(Concept concept, String acknowledgementParamName) {
        List<Community> communities = new ArrayList<>();
        for (Param param : concept.getParams()) {
            //changing the rule by accepting empty ackstmt
            // if (acknowledgementParamName.equals(param.getName()) && StringUtils.isNotBlank(param.getValue())) {
            if (acknowledgementParamName.equals(param.getName())) {
                communities.add(buildCommunity(concept.getId(), concept.getLabel(),
                        StringUtils.isNotBlank(param.getValue()) ? param.getValue() : ""));
            }
        }
        return communities;
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
