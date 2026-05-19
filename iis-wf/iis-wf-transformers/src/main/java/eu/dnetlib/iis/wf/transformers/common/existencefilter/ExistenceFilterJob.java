package eu.dnetlib.iis.wf.transformers.common.existencefilter;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.schemas.Identifier;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * Spark job that filters DocumentContentUrl records keeping only those whose id is present
 * in the provided set of existent identifiers.
 *
 * Replaces the PIG-based existencefilter workflow step.
 */
public class ExistenceFilterJob {

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputFiltered);

            JavaRDD<DocumentContentUrl> data = avroLoader.loadJavaRDD(sc, params.inputData,
                    DocumentContentUrl.class);
            JavaRDD<Identifier> existentIds = avroLoader.loadJavaRDD(sc, params.inputExistentId,
                    Identifier.class);

            JavaPairRDD<String, DocumentContentUrl> dataById = data.mapToPair(
                    d -> new Tuple2<>(d.getId().toString(), d));
            JavaPairRDD<String, Identifier> idsById = existentIds.mapToPair(
                    id -> new Tuple2<>(id.getId().toString(), id));

            JavaRDD<DocumentContentUrl> output = dataById
                    .join(idsById)
                    .map(pair -> pair._2._1);

            avroSaver.saveJavaRDD(output, DocumentContentUrl.SCHEMA$, params.outputFiltered);
        }
    }

    //------------------------ PRIVATE --------------------------

    @Parameters(separators = "=")
    private static class Params {

        @Parameter(names = "-inputData", required = true)
        private String inputData;

        @Parameter(names = "-inputExistentId", required = true)
        private String inputExistentId;

        @Parameter(names = "-outputFiltered", required = true)
        private String outputFiltered;
    }
}
