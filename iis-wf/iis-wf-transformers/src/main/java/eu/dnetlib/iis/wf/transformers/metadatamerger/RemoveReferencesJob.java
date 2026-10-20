package eu.dnetlib.iis.wf.transformers.metadatamerger;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Spark job that clears references from merged document metadata records.
 */
public class RemoveReferencesJob {

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    public static void main(String[] args) throws IOException {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.output);

            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> input = avroLoader.loadJavaRDD(
                    sc, params.input, ExtractedDocumentMetadataMergedWithOriginal.class);
            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> output = input.map(
                    RemoveReferencesJob::removeReferences);

            avroSaver.saveJavaRDD(output, ExtractedDocumentMetadataMergedWithOriginal.SCHEMA$, params.output);
        }
    }

    static ExtractedDocumentMetadataMergedWithOriginal removeReferences(
            ExtractedDocumentMetadataMergedWithOriginal metadata) {
        return ExtractedDocumentMetadataMergedWithOriginal.newBuilder(metadata)
                .setReferences(null)
                .build();
    }

    @Parameters(separators = "=")
    private static class Params {

        @Parameter(names = "-input", required = true)
        private String input;

        @Parameter(names = "-output", required = true)
        private String output;
    }
}
