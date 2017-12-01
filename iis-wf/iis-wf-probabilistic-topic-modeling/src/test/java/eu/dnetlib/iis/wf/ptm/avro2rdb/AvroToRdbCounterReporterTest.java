package eu.dnetlib.iis.wf.ptm.avro2rdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * @author mhorst
 */
@RunWith(MockitoJUnitRunner.class)
public class AvroToRdbCounterReporterTest {

    @InjectMocks
    private AvroToRdbCounterReporter counterReporter = new AvroToRdbCounterReporter();
    
    @Mock
    private SparkAvroSaver avroSaver;
    
    @Mock
    private DataFrame dataFrame;
    
    @Mock
    private JavaRDD<ReportEntry> reportCounters;
    
    @Captor
    private ArgumentCaptor<List<ReportEntry>> reportEntriesCaptor;
    
    
    private static final String reportEntryKey = "key";
    private static final String reportEntryValue = "value";
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void generateCountReportEntry() throws Exception {
        
        // given
        
        String tableName = "someTable";
        long expectedCount = 12;
        when(dataFrame.count()).thenReturn(expectedCount);
        
        // execute
        
        ReportEntry generatedEntry = counterReporter.generateCountReportEntry(dataFrame, tableName);
        
        // assert
        assertEquals("processing.ptm.avro2rdb.sometable.total", generatedEntry.getKey());
        assertTrue(ReportEntryType.COUNTER == generatedEntry.getType());
        assertEquals(String.valueOf(expectedCount), generatedEntry.getValue());
    }
    
    @Test
    public void report() throws Exception {
        
        // given

        List<ReportEntry> reportEntries = Lists.newArrayList(new ReportEntry(reportEntryKey, ReportEntryType.COUNTER, reportEntryValue)); 
        
        JavaSparkContext sparkContext = mock(JavaSparkContext.class);
        String reportPath = "/report/path";
        
        doReturn(reportCounters).when(sparkContext).parallelize(reportEntries);
        
        // execute
        
        counterReporter.report(sparkContext, reportEntries, reportPath);
        
        // assert
        
        verify(sparkContext).parallelize(reportEntriesCaptor.capture());
        
        List<ReportEntry> receivedReportEntries = reportEntriesCaptor.getValue();
        
        assertEquals(1, receivedReportEntries.size());
        
        assertEquals(reportEntryKey, receivedReportEntries.get(0).getKey());
        assertTrue(ReportEntryType.COUNTER == receivedReportEntries.get(0).getType());
        assertEquals(reportEntryValue, receivedReportEntries.get(0).getValue());
        
        verify(avroSaver).saveJavaRDD(reportCounters, ReportEntry.SCHEMA$, reportPath);
        
    }

}
