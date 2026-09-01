package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * Spark driver that validates a SequenceFile on HDFS produced by {@link SequenceFileExporterJob}.
 *
 * <p>Reads the SequenceFile values (JSON-serialised {@link eu.dnetlib.dhp.schema.action.AtomicAction}
 * objects) using the Spark context, counts the actual records, and asserts the count equals the
 * expected value supplied via {@code -expectedCount}.
 *
 * <p>This class is intended to be run in Airflow/Kubernetes test DAGs as the validation step
 * following a {@link SequenceFileExporterJob} invocation.
 *
 * @author mhorst
 */
public class SparkSequenceFileTestConsumer {

    // ----------------------- LOGIC --------------------------------

    public static void main(String[] args) {
        ConsumerParameters params = new ConsumerParameters();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            @SuppressWarnings("unchecked")
            long actualCount = sc
                    .newAPIHadoopFile(
                            params.hdfsInput,
                            SequenceFileInputFormat.class,
                            Text.class,
                            Text.class,
                            sc.hadoopConfiguration())
                    .count();

            if (actualCount != params.expectedCount) {
                throw new AssertionError(String.format(
                        "SequenceFile record count mismatch for '%s': expected %d but got %d",
                        params.hdfsInput, params.expectedCount, actualCount));
            }
        }
    }

    // ----------------------- INNER CLASSES --------------------------------

    @Parameters(separators = "=")
    private static class ConsumerParameters {

        @Parameter(names = "-hdfsInput", required = true,
                description = "HDFS path to the SequenceFile directory or file to validate.")
        private String hdfsInput;

        @Parameter(names = "-expectedCount", required = true,
                description = "Expected number of records in the SequenceFile.")
        private long expectedCount;
    }
}
