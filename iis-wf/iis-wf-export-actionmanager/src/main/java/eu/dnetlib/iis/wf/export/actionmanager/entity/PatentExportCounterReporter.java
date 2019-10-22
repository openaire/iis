package eu.dnetlib.iis.wf.export.actionmanager.entity;

import com.google.common.base.Preconditions;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

import java.util.Arrays;

public class PatentExportCounterReporter {
    public static final String PATENT_REFERENCES_COUNTER = "processing.referenceExtraction.patent.reference";
    public static final String EXPORTED_PATENT_ENTITIES_COUNTER = "export.entity.patent.total";
    public static final String DISTINCT_PUBLICATIONS_WITH_PATENT_REFERENCES_COUNTER = "processing.referenceExtraction.patent.doc";

    private SparkAvroSaver avroSaver = new SparkAvroSaver();

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
