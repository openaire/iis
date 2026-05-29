package eu.dnetlib.iis.common.spark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.TestsIOUtils;
import eu.dnetlib.iis.common.java.io.JsonStreamReader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Generic Spark driver that validates actual HDFS Avro data stores against expected JSON fixtures.
 * <p>
 * Replaces {@code ProcessWrapper + TestingConsumer} for Spark-on-k8s test workflows where HDFS
 * access must go through the {@link JavaSparkContext} rather than a standalone Hadoop
 * {@code Configuration}.
 * <p>
 * Each logical "port" is specified by three parallel list arguments (all must have equal
 * length; entries at the same index describe one port):
 * <ul>
 *   <li>{@code -schemaClass}   — fully qualified name of the generated Avro class
 *       (must have a static {@code SCHEMA$} field)</li>
 *   <li>{@code -classpathJson} — classpath path to the expected JSON fixture, loaded via
 *       {@link ClassPathResourceProvider}</li>
 *   <li>{@code -hdfsInput}     — HDFS path of the actual Avro data store to validate</li>
 * </ul>
 * Records are compared order-independently via {@link TestsIOUtils#assertEqualSets}.
 */
public class SparkAvroTestConsumer {

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        SparkAvroTestConsumerParameters params = new SparkAvroTestConsumerParameters();
        new JCommander(params, args);
        validateParams(params);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            for (int i = 0; i < params.schemaClasses.size(); i++) {
                Schema schema = getSchema(params.schemaClasses.get(i));
                String classpathJson = params.classpathJsons.get(i);
                String hdfsInput = params.hdfsInputs.get(i);

                List<GenericRecord> expected = readFromClasspath(classpathJson, schema);
                List<GenericRecord> actual = loadFromHdfs(sc, hdfsInput, schema).collect();
                TestsIOUtils.assertEqualSets(expected, actual);
            }
        }
    }

    //------------------------ PRIVATE --------------------------

    private static void validateParams(SparkAvroTestConsumerParameters params) {
        int n = params.schemaClasses.size();
        if (params.classpathJsons.size() != n || params.hdfsInputs.size() != n) {
            throw new IllegalArgumentException(String.format(
                    "-schemaClass (%d), -classpathJson (%d) and -hdfsInput (%d) must have equal counts",
                    n, params.classpathJsons.size(), params.hdfsInputs.size()));
        }
        if (n == 0) {
            throw new IllegalArgumentException(
                    "At least one -schemaClass/-classpathJson/-hdfsInput triplet is required");
        }
    }

    private static Schema getSchema(String schemaClassName) {
        try {
            return (Schema) Class.forName(schemaClassName).getField("SCHEMA$").get(null);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Cannot resolve Avro schema for class: " + schemaClassName, e);
        }
    }

    private static List<GenericRecord> readFromClasspath(String classpathJson, Schema schema)
            throws IOException {
        List<GenericRecord> records = new ArrayList<>();
        try (InputStream in = ClassPathResourceProvider.getResourceInputStream(classpathJson);
             JsonStreamReader<GenericRecord> reader = new JsonStreamReader<>(schema, in, GenericRecord.class)) {
            while (reader.hasNext()) {
                records.add(reader.next());
            }
        }
        return records;
    }

    @SuppressWarnings("unchecked")
    private static JavaRDD<GenericRecord> loadFromHdfs(
            JavaSparkContext sc, String hdfsPath, Schema schema) throws IOException {
        Job job = Job.getInstance();
        AvroJob.setInputKeySchema(job, schema);
        JavaPairRDD<AvroKey<GenericRecord>, NullWritable> pairs =
                (JavaPairRDD<AvroKey<GenericRecord>, NullWritable>) sc.newAPIHadoopFile(
                        hdfsPath, AvroKeyInputFormat.class, GenericRecord.class, NullWritable.class,
                        job.getConfiguration());
        return pairs.map(t -> (GenericRecord) GenericData.get().deepCopy(schema, t._1().datum()));
    }

    //------------------------ INNER CLASS --------------------------

    @Parameters(separators = "=")
    private static class SparkAvroTestConsumerParameters {

        @Parameter(names = "-schemaClass", required = true,
                description = "Fully qualified Avro class name (repeatable; one per port)")
        private List<String> schemaClasses = new ArrayList<>();

        @Parameter(names = "-classpathJson", required = true,
                description = "Classpath path to the expected JSON fixture (repeatable; one per port)")
        private List<String> classpathJsons = new ArrayList<>();

        @Parameter(names = "-hdfsInput", required = true,
                description = "HDFS input path of the actual Avro data store (repeatable; one per port)")
        private List<String> hdfsInputs = new ArrayList<>();
    }
}
