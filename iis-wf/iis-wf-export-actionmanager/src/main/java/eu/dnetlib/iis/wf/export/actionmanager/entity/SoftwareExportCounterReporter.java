package eu.dnetlib.iis.wf.export.actionmanager.entity;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple3;

/**
 * Reporter of software entity and relation exporter job counters.<br/>
 * It calculates entities and relation related counters and saves them as {@link ReportEntry} datastore.
 * 
 * @author mhorst
 */
public class SoftwareExportCounterReporter {

    public static final String EXPORTED_SOFTWARE_ENTITIES_COUNTER = "export.entity.software.total";

    public static final String SOFTWARE_REFERENCES_COUNTER = "processing.referenceExtraction.softwareUrl.reference";
    
    public static final String DISTINCT_PUBLICATIONS_WITH_SOFTWARE_REFERENCES_COUNTER = "processing.referenceExtraction.softwareUrl.doc";
    
    private SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Calculates entities and relations related counters based on RDDs and saves them under outputReportPath.
     */
    public void report(JavaSparkContext sparkContext, 
            JavaRDD<?> uniqueEntities, JavaRDD<Tuple3<String, String, Float>> uniqueRelations, String outputReportPath) {
        
        Preconditions.checkNotNull(outputReportPath, "reportPath has not been set");
        Preconditions.checkNotNull(sparkContext, "sparkContext has not been set");
        
        ReportEntry totalEntitiesCounter = ReportEntryFactory.createCounterReportEntry(EXPORTED_SOFTWARE_ENTITIES_COUNTER, uniqueEntities.count());
        ReportEntry totalRelationsCounter = ReportEntryFactory.createCounterReportEntry(SOFTWARE_REFERENCES_COUNTER, uniqueRelations.count());
        ReportEntry distinctPubsCounter = generateDistinctPublicationsCounter(uniqueRelations);
        
        JavaRDD<ReportEntry> report = sparkContext.parallelize(Lists.newArrayList(
                totalEntitiesCounter, totalRelationsCounter, distinctPubsCounter));
        
        avroSaver.saveJavaRDD(report, ReportEntry.SCHEMA$, outputReportPath);
    }
    
    //------------------------ PRIVATE --------------------------
    
    private ReportEntry generateDistinctPublicationsCounter(JavaRDD<Tuple3<String, String, Float>> uniqueRelations) {
        
        long pubsCount = uniqueRelations.map(x -> x._1()).distinct().count();
        
        return ReportEntryFactory.createCounterReportEntry(
                DISTINCT_PUBLICATIONS_WITH_SOFTWARE_REFERENCES_COUNTER, pubsCount);
    }

}

