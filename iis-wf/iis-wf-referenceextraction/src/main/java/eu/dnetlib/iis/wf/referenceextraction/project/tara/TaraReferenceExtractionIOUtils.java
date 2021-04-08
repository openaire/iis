package eu.dnetlib.iis.wf.referenceextraction.project.tara;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameWriter;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TaraReferenceExtractionIOUtils {

    private static final Logger logger = LoggerFactory.getLogger(TaraReferenceExtractionIOUtils.class);

    private TaraReferenceExtractionIOUtils() {
    }

    public static void clearOutput(SparkSession spark,
                                   String outputDocumentToProject) throws IOException {
        clearOutput(outputDocumentToProject, new OutputCleaner(spark));
    }

    public static void clearOutput(String outputDocumentToProject,
                                   OutputCleaner cleaner) throws IOException {
        logger.info("Clearing output location {}.", outputDocumentToProject);
        cleaner.clearOutput(outputDocumentToProject);
    }

    public static void storeInOutput(SparkSession spark,
                                     Dataset<Row> resultsToOutputDF,
                                     String outputDocumentToProject) {
        storeInOutput(resultsToOutputDF, outputDocumentToProject, new AvroDataStoreWriter());
    }

    public static void storeInOutput(Dataset<Row> resultsToOutputDF,
                                     String outputDocumentToProject,
                                     AvroDataStoreWriter writer) {
        logger.info("Storing output data in path {}.", outputDocumentToProject);
        writer.write(resultsToOutputDF, outputDocumentToProject, DocumentToProject.SCHEMA$);
    }

    public static class OutputCleaner {
        private Configuration conf;

        public OutputCleaner(SparkSession spark) {
            this.conf = spark.sparkContext().hadoopConfiguration();
        }

        public void clearOutput(String output) throws IOException {
            HdfsUtils.remove(conf, output);
        }
    }

    public static class AvroDataStoreWriter {
        public void write(Dataset<Row> df, String path, Schema avroSchema) {
            new AvroDataFrameWriter(df).write(path, avroSchema);
        }
    }
}
