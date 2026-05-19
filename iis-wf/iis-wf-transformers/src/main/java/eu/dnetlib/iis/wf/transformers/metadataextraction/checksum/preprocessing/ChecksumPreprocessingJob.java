package eu.dnetlib.iis.wf.transformers.metadataextraction.checksum.preprocessing;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * Spark job that preprocesses DocumentContentUrl records for checksum-based metadata extraction.
 * Filters out records without a contentChecksum, deduplicates by contentChecksum keeping the
 * record with the smallest id, and replaces the id field with the contentChecksum value.
 *
 * Replaces the PIG-based checksum preprocessing transformer workflow step.
 */
public class ChecksumPreprocessingJob {

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.output);

            JavaRDD<DocumentContentUrl> input = avroLoader.loadJavaRDD(sc, params.input,
                    DocumentContentUrl.class);

            // Filter out records without a contentChecksum
            JavaRDD<DocumentContentUrl> withChecksum = input.filter(
                    doc -> doc.getContentChecksum() != null);

            // Group by contentChecksum, keep record with smallest id (stable deduplication)
            JavaPairRDD<String, DocumentContentUrl> byChecksum = withChecksum.mapToPair(
                    doc -> new Tuple2<>(doc.getContentChecksum().toString(), doc));

            JavaPairRDD<String, DocumentContentUrl> dedupedPairs = byChecksum
                    .reduceByKey((a, b) -> a.getId().toString().compareTo(b.getId().toString()) <= 0 ? a : b);

            // Replace id with contentChecksum
            JavaRDD<DocumentContentUrl> output = dedupedPairs.map(pair ->
                    DocumentContentUrl.newBuilder()
                            .setId(pair._2.getContentChecksum())
                            .setUrl(pair._2.getUrl())
                            .setMimeType(pair._2.getMimeType())
                            .setContentChecksum(pair._2.getContentChecksum())
                            .setContentSizeKB(pair._2.getContentSizeKB())
                            .build());

            avroSaver.saveJavaRDD(output, DocumentContentUrl.SCHEMA$, params.output);
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
