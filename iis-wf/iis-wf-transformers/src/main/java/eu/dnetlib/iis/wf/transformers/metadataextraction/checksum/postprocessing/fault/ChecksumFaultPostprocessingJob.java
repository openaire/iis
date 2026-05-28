package eu.dnetlib.iis.wf.transformers.metadataextraction.checksum.postprocessing.fault;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * Spark job that remaps the inputObjectId in Fault records from a contentChecksum back to the
 * original document id. Only DocumentContentUrl records that have a non-null contentChecksum
 * are considered. The fault's inputObjectId is expected to match the DCU's contentChecksum.
 */
public class ChecksumFaultPostprocessingJob {

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.output);

            JavaRDD<Fault> fault = avroLoader.loadJavaRDD(sc, params.inputFault, Fault.class);
            JavaRDD<DocumentContentUrl> dcu = avroLoader.loadJavaRDD(sc, params.inputDocumentContentUrl,
                    DocumentContentUrl.class);

            // Build a mapping: checksum -> original document id
            JavaPairRDD<String, String> checksumToDocId = dcu
                    .filter(d -> d.getContentChecksum() != null)
                    .mapToPair(d -> new Tuple2<>(d.getContentChecksum().toString(), d.getId().toString()));

            // Key faults by their inputObjectId (which is the checksum after preprocessing)
            JavaPairRDD<String, Fault> faultByObjectId = fault.mapToPair(
                    f -> new Tuple2<>(f.getInputObjectId().toString(), f));

            // Inner join: only faults that match a known checksum are remapped
            JavaRDD<Fault> output = checksumToDocId
                    .join(faultByObjectId)
                    .map(pair -> Fault.newBuilder(pair._2._2)
                            .setInputObjectId(pair._2._1)
                            .build());

            avroSaver.saveJavaRDD(output, Fault.SCHEMA$, params.output);
        }
    }

    //------------------------ PRIVATE --------------------------

    @Parameters(separators = "=")
    private static class Params {

        @Parameter(names = "-inputFault", required = true)
        private String inputFault;

        @Parameter(names = "-inputDocumentContentUrl", required = true)
        private String inputDocumentContentUrl;

        @Parameter(names = "-output", required = true)
        private String output;
    }
}
