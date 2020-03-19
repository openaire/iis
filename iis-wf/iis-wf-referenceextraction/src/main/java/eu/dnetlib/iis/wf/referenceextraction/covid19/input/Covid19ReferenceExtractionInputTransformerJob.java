package eu.dnetlib.iis.wf.referenceextraction.covid19.input;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.referenceextraction.covid19.schemas.DocumentMetadata;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * 
 * @author mhorst
 *
 */
public class Covid19ReferenceExtractionInputTransformerJob {
    
    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        Covid19ReferenceExtractionInputTransformerJobParameters params = new Covid19ReferenceExtractionInputTransformerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.output);
            
            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> inputMeta = avroLoader
                    .loadJavaRDD(sc, params.input, ExtractedDocumentMetadataMergedWithOriginal.class);
            
            JavaRDD<DocumentMetadata> output = inputMeta.filter(x -> StringUtils.isNotBlank(x.getTitle()))
                    .map(x -> convertMetadata(x));

            avroSaver.saveJavaRDD(output, DocumentMetadata.SCHEMA$, params.output);
        }
        
    }

    //------------------------ PRIVATE --------------------------
    
    private static DocumentMetadata convertMetadata(ExtractedDocumentMetadataMergedWithOriginal input) {
        DocumentMetadata.Builder metaBuilder = DocumentMetadata.newBuilder();
        metaBuilder.setId(input.getId());
        metaBuilder.setTitle(input.getTitle());
        metaBuilder.setAbstract$(input.getAbstract$());
        if (input.getYear() != null) {
            metaBuilder.setDate(String.valueOf(input.getYear()));    
        }
        return metaBuilder.build();
    }
    
    @Parameters(separators = "=")
    private static class Covid19ReferenceExtractionInputTransformerJobParameters {
        
        @Parameter(names = "-input", required = true)
        private String input;
        
        @Parameter(names = "-output", required = true)
        private String output;
        
    }
}
