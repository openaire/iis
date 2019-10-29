package eu.dnetlib.iis.wf.export.actionmanager.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;

/**
 * Common RDD related utility class.
 */
public class RDDUtils {

    private RDDUtils() {
    }

    /**
     * Saves a given pair RDD as a sequence file.
     *
     * @param pairs               RDD of Text pairs to save.
     * @param numberOfOutputFiles Number of output files.
     * @param outputPath          RDD saving location.
     * @param conf                Hadoop configuration instance.
     */
    public static void saveTextPairRDD(JavaPairRDD<Text, Text> pairs, Integer numberOfOutputFiles, String outputPath, Configuration conf) {
        pairs
                .coalesce(numberOfOutputFiles)
                .saveAsNewAPIHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class, conf);
    }
}
