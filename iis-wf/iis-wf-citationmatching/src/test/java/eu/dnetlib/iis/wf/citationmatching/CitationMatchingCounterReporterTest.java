package eu.dnetlib.iis.wf.citationmatching;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.citationmatching.schemas.Citation;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class CitationMatchingCounterReporterTest {

    @InjectMocks
    private CitationMatchingCounterReporter counterReporter = new CitationMatchingCounterReporter();
    
    @Mock
    private SparkAvroSaver avroSaver;
    
    @Mock
    private JavaSparkContext sparkContext;
    
    
    private String reportPath = "/report/path";
    
    @Mock
    private JavaRDD<Citation> matchedCitations;
    
    @Mock
    private JavaRDD<String> matchedCitationsDocumentIds;
    
    @Mock
    private JavaRDD<String> matchedCitationsDistinctDocumentIds;
    
    @Mock
    private JavaRDD<ReportEntry> reportCounters;
    
    
    @Captor
    private ArgumentCaptor<Function<Citation,String>> extractDocIdFunction;
    
    @Captor
    private ArgumentCaptor<List<ReportEntry>> reportEntriesCaptor;
    
    
    @Before
    public void setup() {
        counterReporter.setReportPath(reportPath);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void report_NULL_SPARK_CONTEXT() {
        // given
        counterReporter.setSparkContext(null);
        
        // execute
        counterReporter.report(matchedCitations);
    }
    
    @Test(expected = NullPointerException.class)
    public void report_NULL_REPORT_PATH() {
        // given
        counterReporter.setReportPath(null);
        
        // execute
        counterReporter.report(matchedCitations);
    }
    
    @Test
    public void report() throws Exception {
        
        // given
        
        when(matchedCitations.count()).thenReturn(14L);
        
        doReturn(matchedCitationsDocumentIds).when(matchedCitations).map(any());
        when(matchedCitationsDocumentIds.distinct()).thenReturn(matchedCitationsDistinctDocumentIds);
        when(matchedCitationsDistinctDocumentIds.count()).thenReturn(3L);
        
        doReturn(reportCounters).when(sparkContext).parallelize(any());
        
        
        // execute
        
        counterReporter.report(matchedCitations);
        
        
        // assert
        
        verify(matchedCitations).map(extractDocIdFunction.capture());
        assertExtractDocIdFunction(extractDocIdFunction.getValue());
        
        verify(sparkContext).parallelize(reportEntriesCaptor.capture());
        assertReportEntries(reportEntriesCaptor.getValue());
        
        verify(avroSaver).saveJavaRDD(reportCounters, ReportEntry.SCHEMA$, reportPath);
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void assertExtractDocIdFunction(Function<Citation,String> function) throws Exception {
        
        Citation citation = new Citation("SOURCE_ID", 2, "DEST_ID", 0.7f);
        
        String docId = function.call(citation);
        
        assertEquals("SOURCE_ID", docId);
    }
    
    private void assertReportEntries(List<ReportEntry> reportEntries) {
        
        assertEquals(2, reportEntries.size());
        
        assertEquals("processing.citationMatching.fuzzy.citDocReferences", reportEntries.get(0).getKey());
        assertEquals("14", reportEntries.get(0).getValue());
        
        assertEquals("processing.citationMatching.fuzzy.docs", reportEntries.get(1).getKey());
        assertEquals("3", reportEntries.get(1).getValue());
    }
    
}
