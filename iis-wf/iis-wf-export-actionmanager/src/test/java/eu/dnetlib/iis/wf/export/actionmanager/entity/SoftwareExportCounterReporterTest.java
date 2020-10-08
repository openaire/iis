package eu.dnetlib.iis.wf.export.actionmanager.entity;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple3;

import java.util.List;

import static eu.dnetlib.iis.wf.export.actionmanager.entity.SoftwareExportCounterReporter.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

/**
 * @author madryk
 */
@ExtendWith(MockitoExtension.class)
public class SoftwareExportCounterReporterTest {

    @InjectMocks
    private SoftwareExportCounterReporter counterReporter = new SoftwareExportCounterReporter();
    
    @Mock
    private SparkAvroSaver avroSaver;
    
    @Mock
    private JavaSparkContext sparkContext;
    
    
    private String outputReportPath = "/report/path";
    
    @Mock
    private JavaRDD<?> uniqueEntities;
    
    @Mock
    private JavaRDD<Tuple3<String, String, Float>> uniqueRelations;
    
    @Mock
    private JavaRDD<String> documentIds;
    
    @Mock
    private JavaRDD<String> distinctDocumentIds;
    
    @Mock
    private JavaRDD<ReportEntry> reportCounters;
    
    
    @Captor
    private ArgumentCaptor<Function<Tuple3<String, String, Float>,String>> extractDocIdFunction;
    
    @Captor
    private ArgumentCaptor<List<ReportEntry>> reportEntriesCaptor;
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void report_NULL_SPARK_CONTEXT() {
        // execute
        assertThrows(NullPointerException.class, () ->
                counterReporter.report(null, uniqueEntities, uniqueRelations, outputReportPath));
    }
    
    @Test
    public void report_NULL_REPORT_PATH() {
        // execute
        assertThrows(NullPointerException.class, () ->
                counterReporter.report(sparkContext, uniqueEntities, uniqueRelations, null));
    }
    
    @Test
    public void report() throws Exception {
        // given
        when(uniqueEntities.count()).thenReturn(10L);
        when(uniqueRelations.count()).thenReturn(5L);
        doReturn(documentIds).when(uniqueRelations).map(any());
        when(documentIds.distinct()).thenReturn(distinctDocumentIds);
        when(distinctDocumentIds.count()).thenReturn(3L);
        doReturn(reportCounters).when(sparkContext).parallelize(any(), eq(1));

        // execute
        counterReporter.report(sparkContext, uniqueEntities, uniqueRelations, outputReportPath);
        
        // assert
        verify(uniqueRelations).map(extractDocIdFunction.capture());
        assertExtractDocIdFunction(extractDocIdFunction.getValue());
        
        verify(sparkContext).parallelize(reportEntriesCaptor.capture(), eq(1));
        assertReportEntries(reportEntriesCaptor.getValue());
        
        verify(avroSaver).saveJavaRDD(reportCounters, ReportEntry.SCHEMA$, outputReportPath);
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void assertExtractDocIdFunction(Function<Tuple3<String, String, Float>,String> function) throws Exception {
        Tuple3<String, String, Float> tuple3 = new Tuple3<>("SOURCE_ID", "DEST_ID", 0.7f);
        String docId = function.call(tuple3);
        assertEquals("SOURCE_ID", docId);
    }
    
    private void assertReportEntries(List<ReportEntry> reportEntries) {
        assertEquals(3, reportEntries.size());
        
        assertEquals(EXPORTED_SOFTWARE_ENTITIES_COUNTER, reportEntries.get(0).getKey());
        assertEquals("10", reportEntries.get(0).getValue());
        
        assertEquals(SOFTWARE_REFERENCES_COUNTER, reportEntries.get(1).getKey());
        assertEquals("5", reportEntries.get(1).getValue());
        
        assertEquals(DISTINCT_PUBLICATIONS_WITH_SOFTWARE_REFERENCES_COUNTER, reportEntries.get(2).getKey());
        assertEquals("3", reportEntries.get(2).getValue());
    }
    
}
