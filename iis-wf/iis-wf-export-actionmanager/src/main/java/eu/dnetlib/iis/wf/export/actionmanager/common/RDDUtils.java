package eu.dnetlib.iis.wf.export.actionmanager.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;

public class RDDUtils {

    private RDDUtils() {
    }

    public static void saveTextPairRDD(JavaPairRDD<Text, Text> pairs, Integer numberOfOutputFiles, String outputPath, Configuration conf) {
        pairs
                .coalesce(numberOfOutputFiles)
                .saveAsNewAPIHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class, conf);
    }
}
