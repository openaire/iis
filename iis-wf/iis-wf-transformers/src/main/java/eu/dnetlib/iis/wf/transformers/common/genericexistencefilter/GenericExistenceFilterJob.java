package eu.dnetlib.iis.wf.transformers.common.genericexistencefilter;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.javamapreduce.hack.AvroSchemaGenerator;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.Identifier;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.SparkSessionFactory;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameReader;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameWriter;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * This is generic filtering job relying on {@link Identifier} records datastore
 * to determine which input records should be written as matched and which are
 * meant to be written as unmatched.
 * 
 * @author mhorst
 *
 */
public class GenericExistenceFilterJob {

    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        
        GenericExistenceFilterJobParameters params = new GenericExistenceFilterJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        try (SparkSession sparkSession = SparkSessionFactory.withConfAndKryo(new SparkConf())) {
            
            Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();
            
            HdfsUtils.remove(hadoopConf, params.outputMatched);
            HdfsUtils.remove(hadoopConf, params.outputUnmatched);
            HdfsUtils.remove(hadoopConf, params.outputReport);
            
            final String idFieldName = params.inputIdFieldName;
            
            final Schema schema = AvroSchemaGenerator.getSchema(params.inputClassName);

            AvroDataFrameReader avroDataFrameReader = new AvroDataFrameReader(sparkSession);
            
            Dataset<Row> inputData = avroDataFrameReader.read(params.input, schema);
            Dataset<Row> inputExistentIdentifiers = avroDataFrameReader.read(params.inputExistentId, Identifier.SCHEMA$).distinct();

            Dataset<Row> unmatched = inputData.join(inputExistentIdentifiers, inputData.col(idFieldName).equalTo(inputExistentIdentifiers.col("id")), "left_anti");
            // reversing the join order to optimize the join starting with significantly smaller dataset
            Dataset<Row> matched = inputExistentIdentifiers.join(inputData, inputData.col(idFieldName).equalTo(inputExistentIdentifiers.col("id")), "inner")
                    .drop(inputExistentIdentifiers.col("id"));
            
            if (!WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(params.matchedRecordsCounterName)) {
                matched.cache();
                SparkAvroSaver reportSaver = new SparkAvroSaver();
                JavaRDD<ReportEntry> reportRdd = generateReportEntries(sparkSession, params.matchedRecordsCounterName,
                        matched.count());
                reportSaver.saveJavaRDD(reportRdd, ReportEntry.SCHEMA$, params.outputReport);
            }

            new AvroDataFrameWriter(matched).write(params.outputMatched, schema);
            new AvroDataFrameWriter(unmatched).write(params.outputUnmatched, schema);
        }
        
    }
    
    //------------------------ PRIVATE --------------------------
    
    private static JavaRDD<ReportEntry> generateReportEntries(SparkSession sparkSession, String reportKey, long recordsCount) {
        return sparkSession
                .createDataset(Lists.newArrayList(ReportEntryFactory.createCounterReportEntry(reportKey, recordsCount)),
                        Encoders.kryo(ReportEntry.class))
                .javaRDD();
    }
    
    @Parameters(separators = "=")
    private static class GenericExistenceFilterJobParameters {
        
        @Parameter(names = "-input", required = true)
        private String input;
        
        @Parameter(names = "-inputIdFieldName", required = true)
        private String inputIdFieldName;
        
        @Parameter(names = "-inputClassName", required = true)
        private String inputClassName;
        
        @Parameter(names = "-inputExistentId", required = true)
        private String inputExistentId;
        
        @Parameter(names = "-outputReport", required = true)
        private String outputReport;
        
        @Parameter(names = "-outputMatched", required = true)
        private String outputMatched;
        
        @Parameter(names = "-outputUnmatched", required = true)
        private String outputUnmatched;
        
        @Parameter(names = "-matchedRecordsCounterName", required = true)
        private String matchedRecordsCounterName;
    }

}

