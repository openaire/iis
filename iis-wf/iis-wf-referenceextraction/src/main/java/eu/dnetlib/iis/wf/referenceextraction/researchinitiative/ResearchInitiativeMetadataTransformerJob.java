package eu.dnetlib.iis.wf.referenceextraction.researchinitiative;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.importer.schemas.Concept;
import eu.dnetlib.iis.importer.schemas.Param;
import eu.dnetlib.iis.referenceextraction.researchinitiative.schemas.ResearchInitiativeMetadata;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * 
 * @author mhorst
 *
 */
public class ResearchInitiativeMetadataTransformerJob {
    
	private static final String PARAM_MINING_LABEL = "miningLabel";
	
    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
    	ResearchInitiativeMetadataTransformerJobParameters params = new ResearchInitiativeMetadataTransformerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        final String whitelistIdentifierRegexp = params.whitelistIdentifierRegexp;
        
        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.output);
            
            JavaRDD<Concept> input = avroLoader.loadJavaRDD(sc, params.input, Concept.class);
            
            JavaRDD<Concept> filteredInput = input.filter(x -> isConceptValid(x, whitelistIdentifierRegexp));
            
            JavaRDD<ResearchInitiativeMetadata> output = filteredInput
                    .map(ResearchInitiativeMetadataTransformerJob::convert);
            
            avroSaver.saveJavaRDD(output, ResearchInitiativeMetadata.SCHEMA$, params.output);
        }
        
    }

    //------------------------ PRIVATE --------------------------
    
    private static boolean isConceptValid(Concept concept, String whitelistIdentifierRegexp) {
    	return Pattern.matches(whitelistIdentifierRegexp, concept.getId());
    }
    
    private static ResearchInitiativeMetadata convert(Concept concept) {
    	ResearchInitiativeMetadata.Builder metaBuilder = ResearchInitiativeMetadata.newBuilder();
        metaBuilder.setId(concept.getId());
        for (Param currentParam : concept.getParams()) {
			if (PARAM_MINING_LABEL.contentEquals(currentParam.getName())) {
				metaBuilder.setLabel(currentParam.getValue());
				return metaBuilder.build();
			}
		}
        metaBuilder.setLabel(concept.getLabel());
        return metaBuilder.build();
    }
    
    @Parameters(separators = "=")
    private static class ResearchInitiativeMetadataTransformerJobParameters {
        
        @Parameter(names = "-input", required = true)
        private String input;
        
        @Parameter(names = "-output", required = true)
        private String output;
        
        @Parameter(names = "-whitelistIdentifierRegexp", required = true)
        private String whitelistIdentifierRegexp;
    }
}
