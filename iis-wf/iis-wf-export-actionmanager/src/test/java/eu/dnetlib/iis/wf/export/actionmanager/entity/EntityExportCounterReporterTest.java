package eu.dnetlib.iis.wf.export.actionmanager.entity;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * @author mhorst
 */
@RunWith(MockitoJUnitRunner.class)
public class EntityExportCounterReporterTest {

    @InjectMocks
    private EntityExportCounterReporter counterReporter = new EntityExportCounterReporter();
    
    @Mock
    private SparkAvroSaver avroSaver;
    
    @Mock
    private JavaSparkContext sparkContext;
    
    
    private String reportPath = "/report/path";
    
    @Mock
    private JavaRDD<?> uniqueEntities;
    
    @Mock
    private JavaRDD<ReportEntry> reportCounters;
    
    @Captor
    private ArgumentCaptor<List<ReportEntry>> reportEntriesCaptor;
    
    
    private static final String counterName = "export.entity.total";
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void report_NULL_SPARK_CONTEXT() {
        // execute
        counterReporter.report(null, uniqueEntities, reportPath, counterName);
    }
    
    @Test(expected = NullPointerException.class)
    public void report_NULL_REPORT_PATH() {
        // execute
        counterReporter.report(sparkContext, uniqueEntities, null, counterName);
    }
    
    @Test
    public void report() throws Exception {
        // given
        Long count = 8L;
        when(uniqueEntities.count()).thenReturn(count);
        doReturn(reportCounters).when(sparkContext).parallelize(any());
        
        // execute
        counterReporter.report(sparkContext, uniqueEntities, reportPath, counterName);
        
        // assert
        verify(sparkContext).parallelize(reportEntriesCaptor.capture());
        assertReportEntries(reportEntriesCaptor.getValue(), count);
        
        verify(avroSaver).saveJavaRDD(reportCounters, ReportEntry.SCHEMA$, reportPath);
        
    }
    
    //------------------------ PRIVATE --------------------------
    
    private void assertReportEntries(List<ReportEntry> reportEntries, Long expectedCount) {
        
        assertEquals(1, reportEntries.size());
        
        assertEquals(counterName, reportEntries.get(0).getKey());
        assertEquals(expectedCount, Long.valueOf(reportEntries.get(0).getValue().toString()));

    }
    
}
