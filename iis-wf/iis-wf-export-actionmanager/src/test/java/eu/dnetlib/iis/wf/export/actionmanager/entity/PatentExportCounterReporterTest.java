package eu.dnetlib.iis.wf.export.actionmanager.entity;

import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.ListTestUtils;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class PatentExportCounterReporterTest {
    private static final String outputReportPath = "/path/to/report";
    private static JavaSparkContext sc;

    @Mock
    private SparkAvroSaver avroSaver;

    @Captor
    private ArgumentCaptor<JavaRDD<ReportEntry>> report;

    @InjectMocks
    private PatentExportCounterReporter reporter = new PatentExportCounterReporter();

    @BeforeClass
    public static void before() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("PatentExportCounterReporterTest");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sc = new JavaSparkContext(conf);
    }

    @AfterClass
    public static void after() {
        sc.stop();
    }

    @Test(expected = NullPointerException.class)
    public void reportShouldThrowExceptionWhenSparkContextIsNull() {
        //given
        JavaPairRDD<Text, Text> relationsToExport = JavaPairRDD.fromJavaRDD(sc.emptyRDD());
        JavaPairRDD<Text, Text> entitiesToExport = JavaPairRDD.fromJavaRDD(sc.emptyRDD());

        //when
        reporter.report(null, relationsToExport, entitiesToExport, outputReportPath);
    }

    @Test(expected = NullPointerException.class)
    public void reportShouldThrowExceptionWhenOutputReportPathIsNull() {
        //given
        JavaPairRDD<Text, Text> relationsToExport = JavaPairRDD.fromJavaRDD(sc.emptyRDD());
        JavaPairRDD<Text, Text> entitiesToExport = JavaPairRDD.fromJavaRDD(sc.emptyRDD());

        //when
        reporter.report(sc, relationsToExport, entitiesToExport, null);
    }

    @Test
    public void reportShouldCreateAndSaveReportAsAvroDatastoreOfReportEntries() {
        //given
        JavaPairRDD<Text, Text> relationsToExport = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>(new Text("R K1"), new Text("R V1")),
                new Tuple2<>(new Text("R K2"), new Text("R V2")),
                new Tuple2<>(new Text("R K3"), new Text("R V3"))));
        JavaPairRDD<Text, Text> entitiesToExport = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>(new Text("E K1"), new Text("E V1")),
                new Tuple2<>(new Text("E K1"), new Text("E V1"))));

        //when
        reporter.report(sc, relationsToExport, entitiesToExport, outputReportPath);

        //then
        verify(avroSaver, times(1)).saveJavaRDD(report.capture(), eq(ReportEntry.SCHEMA$), eq(outputReportPath));
        List<String> actualReportEntriesJson = report.getValue()
                .map(SpecificRecordBase::toString).collect()
                .stream().sorted().collect(Collectors.toList());

        List<ReportEntry> expectedReportEntries = Arrays
                .asList(
                        ReportEntryFactory.createCounterReportEntry(PatentExportCounterReporter.PATENT_REFERENCES_COUNTER, 3),
                        ReportEntryFactory.createCounterReportEntry(PatentExportCounterReporter.EXPORTED_PATENT_ENTITIES_COUNTER, 2),
                        ReportEntryFactory.createCounterReportEntry(PatentExportCounterReporter.DISTINCT_PUBLICATIONS_WITH_PATENT_REFERENCES_COUNTER, 1)
                );
        List<String> expectedReportEntriesJson = expectedReportEntries.stream()
                .map(SpecificRecordBase::toString)
                .sorted()
                .collect(Collectors.toList());
        ListTestUtils
                .compareLists(actualReportEntriesJson, expectedReportEntriesJson);
    }
}