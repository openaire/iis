package eu.dnetlib.iis.wf.export.actionmanager.entity.software;

import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.TestWithSharedSparkContext;
import eu.dnetlib.iis.common.utils.ListTestUtils;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

/**
 * @author madryk
 */
@ExtendWith(MockitoExtension.class)
public class SoftwareExportCounterReporterTest extends TestWithSharedSparkContext {

    private static final String outputReportPath = "/report/path";

    @Mock
    private SparkAvroSaver avroSaver;

    @Captor
    private ArgumentCaptor<JavaRDD<ReportEntry>> reportEntriesCaptor;

    @InjectMocks
    private final SoftwareExportCounterReporter counterReporter = new SoftwareExportCounterReporter();

    //------------------------ TESTS --------------------------

    @Test
    public void report_NULL_SPARK_CONTEXT() {
        // execute
        assertThrows(NullPointerException.class, () ->
                counterReporter.report(null, jsc().emptyRDD(), outputReportPath));
    }

    @Test
    public void report_NULL_REPORT_PATH() {
        // execute
        assertThrows(NullPointerException.class, () ->
                counterReporter.report(jsc(), jsc().emptyRDD(), null));
    }

    @Test
    public void report() throws Exception {
        // given
        JavaRDD<SoftwareExportMetadata> rdd = jsc().parallelize(Arrays.asList(
                new SoftwareExportMetadata(null, null, "d1", "s1", null),
                new SoftwareExportMetadata(null, null, "d1", "s2", null),
                new SoftwareExportMetadata(null, null, "d2", "s2", null)
                )
        );

        // execute
        counterReporter.report(jsc(), rdd, outputReportPath);

        // assert
        verify(avroSaver, times(1))
                .saveJavaRDD(reportEntriesCaptor.capture(), eq(ReportEntry.SCHEMA$), eq(outputReportPath));
        List<String> actualReportEntriesAsJson = reportEntriesCaptor.getValue()
                .map(SpecificRecordBase::toString).collect()
                .stream().sorted().collect(Collectors.toList());
        List<String> expectedReportEntriesAsJson = Stream.of(
                ReportEntryFactory.createCounterReportEntry(SoftwareExportCounterReporter.EXPORTED_SOFTWARE_ENTITIES_COUNTER, 2),
                ReportEntryFactory.createCounterReportEntry(SoftwareExportCounterReporter.SOFTWARE_REFERENCES_COUNTER, 3),
                ReportEntryFactory.createCounterReportEntry(SoftwareExportCounterReporter.DISTINCT_PUBLICATIONS_WITH_SOFTWARE_REFERENCES_COUNTER, 2)
        )
                .map(SpecificRecordBase::toString)
                .sorted()
                .collect(Collectors.toList());
        ListTestUtils.compareLists(actualReportEntriesAsJson, expectedReportEntriesAsJson);
    }
}
