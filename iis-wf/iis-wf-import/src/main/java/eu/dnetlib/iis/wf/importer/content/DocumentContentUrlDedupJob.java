package eu.dnetlib.iis.wf.importer.content;

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
 * Spark port of the {@code dedup.pig} script executed by the Oozie
 * {@code eu/dnetlib/iis/wf/importer/content_url/dedup/oozie_app} workflow.
 *
 * <p>Deduplicates {@link DocumentContentUrl} records by the (id, contentChecksum) pair keeping
 * an arbitrary single record per group, exactly as the original PIG script did
 * ({@code GROUP data BY (id, contentChecksum); LIMIT data 1}).</p>
 */
public class DocumentContentUrlDedupJob {

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    private DocumentContentUrlDedupJob() {
    }

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.output);

            JavaRDD<DocumentContentUrl> input = avroLoader.loadJavaRDD(sc, params.input,
                    DocumentContentUrl.class);

            JavaPairRDD<Tuple2<CharSequence, CharSequence>, DocumentContentUrl> byIdAndChecksum = input
                    .mapToPair(doc -> new Tuple2<>(new Tuple2<>(doc.getId(), doc.getContentChecksum()), doc));

            JavaRDD<DocumentContentUrl> deduped = byIdAndChecksum
                    .reduceByKey((a, b) -> a)
                    .values();

            avroSaver.saveJavaRDD(deduped, DocumentContentUrl.SCHEMA$, params.output);
        }
    }

    //------------------------ PARAMETERS --------------------------

    @Parameters(separators = "=")
    public static class Params {

        @Parameter(names = "-input", required = true)
        public String input;

        @Parameter(names = "-output", required = true)
        public String output;
    }
}
