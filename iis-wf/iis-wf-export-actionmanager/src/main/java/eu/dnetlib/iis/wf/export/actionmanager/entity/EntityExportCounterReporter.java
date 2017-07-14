package eu.dnetlib.iis.wf.export.actionmanager.entity;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Reporter of entity exporter job counters.<br/>
 * It calculates entities related counters and saves them as {@link ReportEntry} datastore.
 * 
 * @author mhorst
 */
public class EntityExportCounterReporter {

    private SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Calculates entities related counters based on entities text RDD and saves them under outputReportPath.
     */
    public void report(JavaSparkContext sparkContext, JavaRDD<?> uniqueEntities, 
            String outputReportPath, String counterName) {
        
        Preconditions.checkNotNull(outputReportPath, "reportPath has not been set");
        Preconditions.checkNotNull(sparkContext, "sparkContext has not been set");
        
        ReportEntry totalEntitiesCounter = ReportEntryFactory.createCounterReportEntry(counterName, uniqueEntities.count());
        
        JavaRDD<ReportEntry> report = sparkContext.parallelize(Lists.newArrayList(totalEntitiesCounter));
        
        avroSaver.saveJavaRDD(report, ReportEntry.SCHEMA$, outputReportPath);
    }

}

