package eu.dnetlib.iis.wf.export.actionmanager.entity.software;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * Reporter of software entity and relation exporter job counters.<br/>
 * It calculates entities and relation related counters and saves them as {@link ReportEntry} datastore.
 *
 * @author mhorst
 */
public class SoftwareExportCounterReporter {

    public static final String EXPORTED_SOFTWARE_ENTITIES_COUNTER = "export.entities.software";

    public static final String SOFTWARE_REFERENCES_COUNTER = "processing.referenceExtraction.softwareUrl.references";

    public static final String DISTINCT_PUBLICATIONS_WITH_SOFTWARE_REFERENCES_COUNTER = "processing.referenceExtraction.softwareUrl.docs";

    private SparkAvroSaver avroSaver = new SparkAvroSaver();


    //------------------------ LOGIC --------------------------

    /**
     * Calculates entities and relations related counters based on RDDs and saves them under outputReportPath.
     */
    public void report(JavaSparkContext sc, JavaRDD<SoftwareExportMetadata> rdd, String outputReportPath) {
        Preconditions.checkNotNull(sc, "sparkContext has not been set");
        Preconditions.checkNotNull(outputReportPath, "reportPath has not been set");

        ReportEntry totalEntitiesCounter = ReportEntryFactory.createCounterReportEntry(
                EXPORTED_SOFTWARE_ENTITIES_COUNTER, totalEntitiesCount(rdd));
        ReportEntry totalRelationsCounter = ReportEntryFactory.createCounterReportEntry(
                SOFTWARE_REFERENCES_COUNTER, totalRelationsCount(rdd));
        ReportEntry distinctPublicationsCounter = ReportEntryFactory.createCounterReportEntry
                (DISTINCT_PUBLICATIONS_WITH_SOFTWARE_REFERENCES_COUNTER, distinctPublicationsCount(rdd));

        JavaRDD<ReportEntry> report = sc.parallelize(Lists.newArrayList(
                totalEntitiesCounter, totalRelationsCounter, distinctPublicationsCounter), 1);

        avroSaver.saveJavaRDD(report, ReportEntry.SCHEMA$, outputReportPath);
    }

    //------------------------ PRIVATE --------------------------

    private long totalEntitiesCount(JavaRDD<SoftwareExportMetadata> rdd) {
        return rdd
                .map(SoftwareExportMetadata::getSoftwareId)
                .distinct()
                .count();
    }

    private long totalRelationsCount(JavaRDD<SoftwareExportMetadata> rdd) {
        return rdd
                .mapToPair(x -> new Tuple2<>(x.getDocumentId(), x.getSoftwareId()))
                .distinct()
                .count();
    }

    private long distinctPublicationsCount(JavaRDD<SoftwareExportMetadata> rdd) {
        return rdd
                .map(SoftwareExportMetadata::getDocumentId)
                .distinct()
                .count();
    }
}
