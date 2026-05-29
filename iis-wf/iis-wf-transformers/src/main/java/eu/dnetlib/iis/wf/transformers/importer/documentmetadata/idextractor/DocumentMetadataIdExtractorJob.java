package eu.dnetlib.iis.wf.transformers.importer.documentmetadata.idextractor;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.common.schemas.Identifier;
import eu.dnetlib.iis.importer.schemas.DocumentMetadata;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Spark job that extracts document identifiers from DocumentMetadata records.
 */
public class DocumentMetadataIdExtractorJob {

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputIdentifier);

            JavaRDD<DocumentMetadata> input = avroLoader.loadJavaRDD(sc, params.inputDocumentMetadata,
                    DocumentMetadata.class);

            JavaRDD<Identifier> output = input.map(doc ->
                    Identifier.newBuilder()
                            .setId(doc.getId())
                            .build());

            avroSaver.saveJavaRDD(output, Identifier.SCHEMA$, params.outputIdentifier);
        }
    }

    //------------------------ PRIVATE --------------------------

    @Parameters(separators = "=")
    private static class Params {

        @Parameter(names = "-inputDocumentMetadata", required = true)
        private String inputDocumentMetadata;

        @Parameter(names = "-outputIdentifier", required = true)
        private String outputIdentifier;
    }
}
