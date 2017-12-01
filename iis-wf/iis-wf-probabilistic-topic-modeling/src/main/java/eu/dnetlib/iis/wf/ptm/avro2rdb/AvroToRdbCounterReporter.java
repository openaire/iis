package eu.dnetlib.iis.wf.ptm.avro2rdb;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;

import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Module responsible 
 * 
 * @author mhorst
 *
 */
public class AvroToRdbCounterReporter {

    private static final String REPORT_ENTRY_PREFIX = "processing.ptm.avro2rdb.";
    
    private static final String REPORT_ENTRY_TOTAL_SUFFIX = ".total";
    
    private SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Generates RDD with reports provided as input and saves them under outputReportPath.
     */
    public void report(JavaSparkContext sparkContext, List<ReportEntry> reportEntries, String outputReportPath) {
        avroSaver.saveJavaRDD(sparkContext.parallelize(reportEntries), ReportEntry.SCHEMA$, outputReportPath);
    }
    
    /**
     * Generates report entry with count value obtained from given rdd using the key provided as parameter.
     */
    protected ReportEntry generateCountReportEntry(DataFrame dataFrame, String tableName) {
        return ReportEntryFactory.createCounterReportEntry(
                REPORT_ENTRY_PREFIX + tableName.toLowerCase() + REPORT_ENTRY_TOTAL_SUFFIX, dataFrame.count());
    }

}
