package eu.dnetlib.iis.wf.export.actionmanager.entity.patent;

import com.google.common.base.Preconditions;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

import java.util.Arrays;

/**
 * Reporter of patent entity and relation exporter job counters.<br/>
 * It calculates entities and relation related counters and saves them as {@link ReportEntry} datastore.
 *
 * @author mhorst
 */
public class PatentExportCounterReporter {
    public static final String PATENT_REFERENCES_COUNTER = "processing.referenceExtraction.patent.reference";
    public static final String EXPORTED_PATENT_ENTITIES_COUNTER = "export.entity.patent.total";
    public static final String DISTINCT_PUBLICATIONS_WITH_PATENT_REFERENCES_COUNTER = "processing.referenceExtraction.patent.doc";

    private SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------

    /**
     * Calculates entities and relations related counters based on RDDs and saves them under outputReportPath.
     *
     * @param sc                SparkContext instance.
     * @param relationsToExport Pair RDD of exported relations.
     * @param entitiesToExport  Pair RDD of exported entities.
     * @param outputReportPath  Path to report saving location.
     */
    public void report(JavaSparkContext sc,
                       JavaPairRDD<Text, Text> relationsToExport,
                       JavaPairRDD<Text, Text> entitiesToExport,
                       String outputReportPath) {
        Preconditions.checkNotNull(sc, "sparkContext has not been set");
        Preconditions.checkNotNull(outputReportPath, "reportPath has not been set");

        ReportEntry totalRelationsCounter = ReportEntryFactory
                .createCounterReportEntry(PATENT_REFERENCES_COUNTER, relationsToExport.count());
        ReportEntry totalEntitiesCounter = ReportEntryFactory
                .createCounterReportEntry(EXPORTED_PATENT_ENTITIES_COUNTER, entitiesToExport.count());
        ReportEntry distinctPublicationsCounter = ReportEntryFactory
                .createCounterReportEntry(DISTINCT_PUBLICATIONS_WITH_PATENT_REFERENCES_COUNTER, entitiesToExport.map(x -> x._2.toString()).distinct().count());

        JavaRDD<ReportEntry> report = sc.parallelize(Arrays.asList(
                totalRelationsCounter, totalEntitiesCounter, distinctPublicationsCounter), 1);

        avroSaver.saveJavaRDD(report, ReportEntry.SCHEMA$, outputReportPath);
    }
}
