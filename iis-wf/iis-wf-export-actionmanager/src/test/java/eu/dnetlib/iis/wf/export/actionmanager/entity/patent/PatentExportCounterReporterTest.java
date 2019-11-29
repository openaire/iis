package eu.dnetlib.iis.wf.export.actionmanager.entity.patent;

import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.ListTestUtils;
import eu.dnetlib.iis.referenceextraction.patent.schemas.DocumentToPatent;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.spark.SparkConf;
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
        conf.set("spark.driver.host", "localhost");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.setAppName("PatentExportCounterReporterTest");
        sc = new JavaSparkContext(conf);
    }

    @AfterClass
    public static void after() {
        sc.stop();
    }

    @Test(expected = NullPointerException.class)
    public void reportShouldThrowExceptionWhenSparkContextIsNull() {
        //given
        JavaRDD<DocumentToPatentWithIdsToExport> documentToPatentsToExportWithIds = sc.emptyRDD();

        //when
        reporter.report(null, documentToPatentsToExportWithIds, outputReportPath);
    }

    @Test(expected = NullPointerException.class)
    public void reportShouldThrowExceptionWhenOutputReportPathIsNull() {
        //given
        JavaRDD<DocumentToPatentWithIdsToExport> documentToPatentsToExportWithIds = sc.emptyRDD();

        //when
        reporter.report(sc, documentToPatentsToExportWithIds, null);
    }

    @Test
    public void reportShouldCreateAndSaveReportAsAvroDatastoreOfReportEntries() {
        //given
        List<DocumentToPatent> documentToPatents = Arrays.asList(
                DocumentToPatent.newBuilder().setDocumentId("d1").setPatentId("p1").setConfidenceLevel(0.9f).build(),
                DocumentToPatent.newBuilder().setDocumentId("d1").setPatentId("p2").setConfidenceLevel(0.9f).build(),
                DocumentToPatent.newBuilder().setDocumentId("d2").setPatentId("p2").setConfidenceLevel(0.9f).build()
        );
        JavaRDD<DocumentToPatentWithIdsToExport> documentToPatentsToExportWithIds = sc.parallelize(documentToPatents)
                .map(x ->
                        new DocumentToPatentWithIdsToExport(x, String.format("export_%s", x.getDocumentId()), String.format("export_%s", x.getPatentId())));

        //when
        reporter.report(sc, documentToPatentsToExportWithIds, outputReportPath);

        //then
        verify(avroSaver, times(1)).saveJavaRDD(report.capture(), eq(ReportEntry.SCHEMA$), eq(outputReportPath));
        List<String> actualReportEntriesJson = report.getValue()
                .map(SpecificRecordBase::toString).collect()
                .stream().sorted().collect(Collectors.toList());
        List<ReportEntry> expectedReportEntries = Arrays
                .asList(
                        ReportEntryFactory.createCounterReportEntry(PatentExportCounterReporter.PATENT_REFERENCES_COUNTER, 3),
                        ReportEntryFactory.createCounterReportEntry(PatentExportCounterReporter.EXPORTED_PATENT_ENTITIES_COUNTER, 2),
                        ReportEntryFactory.createCounterReportEntry(PatentExportCounterReporter.DISTINCT_PUBLICATIONS_WITH_PATENT_REFERENCES_COUNTER, 2)
                );
        List<String> expectedReportEntriesJson = expectedReportEntries.stream()
                .map(SpecificRecordBase::toString)
                .sorted()
                .collect(Collectors.toList());
        ListTestUtils
                .compareLists(actualReportEntriesJson, expectedReportEntriesJson);
    }
}
