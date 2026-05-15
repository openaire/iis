package eu.dnetlib.iis.common.spark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.java.io.JsonStreamReader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Generic Spark driver that reads Avro records from JSON fixtures on the JAR classpath
 * and writes them to HDFS as Avro data stores.
 * <p>
 * Replaces {@code ProcessWrapper + Producer} for Spark-on-k8s test workflows where HDFS
 * access must go through the {@link JavaSparkContext} rather than a standalone Hadoop
 * {@code Configuration}.
 * <p>
 * Each logical "port" is specified by three parallel list arguments (all must have equal
 * length; entries at the same index describe one port):
 * <ul>
 *   <li>{@code -schemaClass}   — fully qualified name of the generated Avro class
 *       (must have a static {@code SCHEMA$} field)</li>
 *   <li>{@code -classpathJson} — classpath path to the JSON fixture, loaded via
 *       {@link ClassPathResourceProvider}</li>
 *   <li>{@code -hdfsOutput}    — HDFS path where the Avro data store is written</li>
 * </ul>
 */
public class SparkAvroTestProducer {

    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        SparkAvroTestProducerParameters params = new SparkAvroTestProducerParameters();
        new JCommander(params, args);
        validateParams(params);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            for (int i = 0; i < params.schemaClasses.size(); i++) {
                Schema schema = getSchema(params.schemaClasses.get(i));
                String classpathJson = params.classpathJsons.get(i);
                String hdfsOutput = params.hdfsOutputs.get(i);

                List<GenericRecord> records = readFromClasspath(classpathJson, schema);
                HdfsUtils.remove(sc.hadoopConfiguration(), hdfsOutput);
                avroSaver.saveJavaRDD(sc.parallelize(records), schema, hdfsOutput);
            }
        }
    }

    //------------------------ PRIVATE --------------------------

    private static void validateParams(SparkAvroTestProducerParameters params) {
        int n = params.schemaClasses.size();
        if (params.classpathJsons.size() != n || params.hdfsOutputs.size() != n) {
            throw new IllegalArgumentException(String.format(
                    "-schemaClass (%d), -classpathJson (%d) and -hdfsOutput (%d) must have equal counts",
                    n, params.classpathJsons.size(), params.hdfsOutputs.size()));
        }
        if (n == 0) {
            throw new IllegalArgumentException(
                    "At least one -schemaClass/-classpathJson/-hdfsOutput triplet is required");
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

    //------------------------ INNER CLASS --------------------------

    @Parameters(separators = "=")
    private static class SparkAvroTestProducerParameters {

        @Parameter(names = "-schemaClass", required = true,
                description = "Fully qualified Avro class name (repeatable; one per port)")
        private List<String> schemaClasses = new ArrayList<>();

        @Parameter(names = "-classpathJson", required = true,
                description = "Classpath path to the JSON fixture (repeatable; one per port)")
        private List<String> classpathJsons = new ArrayList<>();

        @Parameter(names = "-hdfsOutput", required = true,
                description = "HDFS output path for the Avro data store (repeatable; one per port)")
        private List<String> hdfsOutputs = new ArrayList<>();
    }
}
