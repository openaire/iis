package eu.dnetlib.iis.wf.transformers.metadataextraction.documenttext;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Spark job that extracts the text field from ExtractedDocumentMetadata records.
 */
public class DocumentTextTransformerJob {

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.output);

            JavaRDD<ExtractedDocumentMetadata> input = avroLoader.loadJavaRDD(sc, params.input,
                    ExtractedDocumentMetadata.class);

            JavaRDD<DocumentText> output = input.map(doc ->
                    DocumentText.newBuilder()
                            .setId(doc.getId())
                            .setText(doc.getText())
                            .build());

            avroSaver.saveJavaRDD(output, DocumentText.SCHEMA$, params.output);
        }
    }

    //------------------------ PRIVATE --------------------------

    @Parameters(separators = "=")
    private static class Params {

        @Parameter(names = "-input", required = true)
        private String input;

        @Parameter(names = "-output", required = true)
        private String output;
    }
}
