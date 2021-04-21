package eu.dnetlib.iis.wf.export.actionmanager.entity.patent;

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

@ExtendWith(MockitoExtension.class)
public class PatentExportCounterReporterTest extends TestWithSharedSparkContext {
    private static final String outputReportPath = "/path/to/report";

    @Mock
    private SparkAvroSaver avroSaver;

    @Captor
    private ArgumentCaptor<JavaRDD<ReportEntry>> reportEntriesCaptor;

    @InjectMocks
    private final PatentExportCounterReporter counterReporter = new PatentExportCounterReporter();

    @Test
    public void reportShouldThrowExceptionWhenSparkContextIsNull() {
        //when
        assertThrows(NullPointerException.class, () ->
                counterReporter.report(null, jsc().emptyRDD(), outputReportPath));
    }

    @Test
    public void reportShouldThrowExceptionWhenOutputReportPathIsNull() {
        //when
        assertThrows(NullPointerException.class, () ->
                counterReporter.report(jsc(), jsc().emptyRDD(), null));
    }

    @Test
    public void reportShouldCreateAndSaveReportAsAvroDatastoreOfReportEntries() {
        //given
        JavaRDD<PatentExportMetadata> rdd = jsc().parallelize(Arrays.asList(
                new PatentExportMetadata(null, null, "d1", "p1"),
                new PatentExportMetadata(null, null, "d1", "p2"),
                new PatentExportMetadata(null, null, "d2", "p2")
                )
        );

        //when
        counterReporter.report(jsc(), rdd, outputReportPath);

        //then
        verify(avroSaver, times(1))
                .saveJavaRDD(reportEntriesCaptor.capture(), eq(ReportEntry.SCHEMA$), eq(outputReportPath));
        List<String> actualReportEntriesAsJson = reportEntriesCaptor.getValue()
                .map(SpecificRecordBase::toString).collect()
                .stream().sorted().collect(Collectors.toList());
        List<String> expectedReportEntriesAsJson = Stream.of(
                ReportEntryFactory.createCounterReportEntry(PatentExportCounterReporter.EXPORTED_PATENT_ENTITIES_COUNTER, 2),
                ReportEntryFactory.createCounterReportEntry(PatentExportCounterReporter.PATENT_REFERENCES_COUNTER, 3),
                ReportEntryFactory.createCounterReportEntry(PatentExportCounterReporter.DISTINCT_PUBLICATIONS_WITH_PATENT_REFERENCES_COUNTER, 2)
        )
                .map(SpecificRecordBase::toString)
                .sorted()
                .collect(Collectors.toList());
        ListTestUtils.compareLists(actualReportEntriesAsJson, expectedReportEntriesAsJson);
    }
}
