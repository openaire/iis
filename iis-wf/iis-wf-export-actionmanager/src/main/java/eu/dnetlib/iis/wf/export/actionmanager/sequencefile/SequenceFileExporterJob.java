package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

import java.util.HashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.common.utils.RDDUtils;
import eu.dnetlib.iis.wf.export.actionmanager.AtomicActionSerializationUtils;
import eu.dnetlib.iis.wf.export.actionmanager.module.ActionBuilderFactory;
import eu.dnetlib.iis.wf.export.actionmanager.module.ActionBuilderModule;
import eu.dnetlib.iis.wf.export.actionmanager.module.TrustLevelThresholdExceededException;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import scala.Tuple2;

/**
 * Spark 4 replacement for the MapReduce-based {@link SequenceFileExporterMapper}.
 *
 * <p>Reads Avro input records, converts them to {@link AtomicAction} objects via a configurable
 * {@link ActionBuilderFactory}, and writes the JSON-serialised actions as a SequenceFile with
 * BLOCK-level compression, preserving the output contract expected by downstream consumers.
 *
 * <p>The job is intended to be invoked once per export action type. The Airflow DAG submits
 * multiple instances in parallel, one for each configured inference type, mirroring the fork/join
 * structure of the original Oozie workflow.
 *
 * @author mhorst
 */
public class SequenceFileExporterJob {

    // ----------------------- LOGIC --------------------------------

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        JobParameters params = new JobParameters();
        new JCommander(params).parse(args);

        if (WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(params.inputPath)) {
            // input path is not defined for this run; nothing to export
            return;
        }

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputPath);

            // Capture serialisable snapshots of the driver-side state for use on executors.
            final Map<String, String> confMap = new HashMap<>(params.dynamicParams);
            final String factoryClassName = params.actionBuilderFactoryClassName;
            final String schemaClassName = params.inputAvroSchemaClass;

            // Resolve the concrete Avro record class on the driver (fast-fail on typos).
            Class<? extends SpecificRecordBase> avroClass =
                    (Class<? extends SpecificRecordBase>) Class.forName(schemaClassName);

            // Read all records from the Avro datastore.
            JavaPairRDD<Text, Text> actionPairs = new SparkAvroLoader()
                    .loadJavaRDD(sc, params.inputPath, avroClass)
                    .mapPartitionsToPair(records -> {
                        // Instantiate the factory and module once per executor partition,
                        // mirroring the Mapper#setup() / Mapper#map() lifecycle.
                        Configuration localConf = new Configuration(false);
                        confMap.forEach(localConf::set);

                        ActionBuilderFactory<SpecificRecordBase, Oaf> factory =
                                (ActionBuilderFactory<SpecificRecordBase, Oaf>) Class.forName(factoryClassName)
                                        .getConstructor()
                                        .newInstance();
                        ActionBuilderModule<SpecificRecordBase, Oaf> module = factory.instantiate(localConf);
                        ObjectMapper objectMapper = new ObjectMapper();

                        // Use a lazy stream so pairs are produced on-demand rather than
                        // accumulated in an ArrayList before being handed to the shuffle writer.
                        // This prevents holding an entire partition's output in heap simultaneously,
                        // which caused OOM in Celeborn's PushTask allocations on large partitions.
                        return StreamSupport
                                .stream(Spliterators.spliteratorUnknownSize(records, Spliterator.ORDERED), false)
                                .flatMap(datum -> {
                                    try {
                                        return module.build(datum).stream()
                                                .map(action -> {
                                                    try {
                                                        return new Tuple2<>(new Text(""),
                                                                new Text(AtomicActionSerializationUtils
                                                                        .serializeAction(action, objectMapper)));
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                });
                                    } catch (TrustLevelThresholdExceededException e) {
                                        return Stream.empty();
                                    }
                                })
                                .iterator();
                    });

            // Build the output configuration with BLOCK-level compression, matching the
            // original Oozie workflow settings (mapreduce.output.fileoutputformat.compress.type=BLOCK).
            Configuration outConf = new Configuration(sc.hadoopConfiguration());
            outConf.set("mapreduce.output.fileoutputformat.compress", "true");
            outConf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");

            if (params.numberOfOutputFiles > 0) {
                // Repartition to coalesce many small input files into a bounded number of output
                // parts, equivalent to mapreduce.job.reduces > 0 in the original workflow.
                RDDUtils.saveTextPairRDD(actionPairs, params.numberOfOutputFiles, params.outputPath, outConf);
            } else {
                // Preserve the natural partition structure of the input, equivalent to
                // mapreduce.job.reduces=0 (map-only job) in the original workflow.
                RDDUtils.saveTextPairRDD(actionPairs, params.outputPath, outConf);
            }
        }
    }

    // ----------------------- INNER CLASSES --------------------------------

    @Parameters(separators = "=")
    private static class JobParameters {

        @Parameter(names = "-inputPath", required = true,
                description = "HDFS path to the Avro input datastore. "
                        + "Pass $UNDEFINED$ to skip this export action.")
        private String inputPath;

        @Parameter(names = "-outputPath", required = true,
                description = "HDFS output path for the SequenceFile result.")
        private String outputPath;

        @Parameter(names = "-actionBuilderFactoryClassName", required = true,
                description = "Fully-qualified class name of the ActionBuilderFactory implementation "
                        + "responsible for converting input Avro records to AtomicAction objects.")
        private String actionBuilderFactoryClassName;

        @Parameter(names = "-inputAvroSchemaClass", required = true,
                description = "Fully-qualified class name of the Avro SpecificRecord input type "
                        + "(must match the record type produced by the ActionBuilderFactory).")
        private String inputAvroSchemaClass;

        @Parameter(names = "-numberOfOutputFiles", required = false,
                description = "Target number of SequenceFile output parts after repartitioning. "
                        + "When > 0 the RDD is repartitioned (equivalent to mapreduce.job.reduces > 0). "
                        + "When 0 (default) the natural partition structure is preserved "
                        + "(equivalent to mapreduce.job.reduces=0, i.e. map-only execution).")
        private int numberOfOutputFiles = 0;

        @DynamicParameter(names = "-D",
                description = "Additional Hadoop configuration entries forwarded to the ActionBuilderFactory "
                        + "(e.g. trust-level thresholds, collectedfrom key, similarity threshold, PDB URL root). "
                        + "Example: -Dexport.trust.level.threshold=0.5")
        private Map<String, String> dynamicParams = new HashMap<>();
    }
}
