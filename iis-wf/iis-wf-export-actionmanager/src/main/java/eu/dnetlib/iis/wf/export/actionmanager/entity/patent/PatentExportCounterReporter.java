package eu.dnetlib.iis.wf.export.actionmanager.entity.patent;

import com.google.common.base.Preconditions;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Reporter of patent entity and relation exporter job counters.<br/>
 * It calculates entities and relation related counters and saves them as {@link ReportEntry} datastore.
 *
 * @author mhorst
 */
public class PatentExportCounterReporter {

    public static final String EXPORTED_PATENT_ENTITIES_COUNTER = "export.entities.patent";

    public static final String PATENT_REFERENCES_COUNTER = "processing.referenceExtraction.patent.references";

    public static final String DISTINCT_PUBLICATIONS_WITH_PATENT_REFERENCES_COUNTER = "processing.referenceExtraction.patent.docs";

    private SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------

    /**
     * Calculates entities and relations related counters based on RDDs and saves them under outputReportPath.
     *
     * @param sc               SparkContext instance.
     * @param rdd              RDD of exported document to patents with ids.
     * @param outputReportPath Path to report saving location.
     */
    public void report(JavaSparkContext sc, JavaRDD<PatentExportMetadata> rdd, String outputReportPath) {
        Preconditions.checkNotNull(sc, "sparkContext has not been set");
        Preconditions.checkNotNull(outputReportPath, "reportPath has not been set");

        ReportEntry totalEntitiesCounter = ReportEntryFactory.createCounterReportEntry(
                EXPORTED_PATENT_ENTITIES_COUNTER, totalEntitiesCount(rdd));
        ReportEntry totalRelationsCounter = ReportEntryFactory.createCounterReportEntry(
                PATENT_REFERENCES_COUNTER, totalRelationsCount(rdd));
        ReportEntry distinctPublicationsCounter = ReportEntryFactory.createCounterReportEntry(
                DISTINCT_PUBLICATIONS_WITH_PATENT_REFERENCES_COUNTER, distinctPublicationsCount(rdd));

        JavaRDD<ReportEntry> report = sc.parallelize(Arrays.asList(
                totalRelationsCounter, totalEntitiesCounter, distinctPublicationsCounter), 1);

        avroSaver.saveJavaRDD(report, ReportEntry.SCHEMA$, outputReportPath);
    }

    //------------------------ PRIVATE --------------------------

    private long totalEntitiesCount(JavaRDD<PatentExportMetadata> documentToPatentsToExportWithIds) {
        return documentToPatentsToExportWithIds
                .map(PatentExportMetadata::getPatentId)
                .distinct()
                .count();
    }

    private long totalRelationsCount(JavaRDD<PatentExportMetadata> documentToPatentsToExportWithIds) {
        return documentToPatentsToExportWithIds
                .mapToPair(x -> new Tuple2<>(x.getDocumentId(), x.getPatentId()))
                .distinct()
                .count();
    }

    private long distinctPublicationsCount(JavaRDD<PatentExportMetadata> documentToPatentsToExportWithIds) {
        return documentToPatentsToExportWithIds
                .map(PatentExportMetadata::getDocumentId)
                .distinct()
                .count();
    }
}
