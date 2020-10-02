package eu.dnetlib.iis.wf.citationmatching;

import eu.dnetlib.iis.citationmatching.schemas.Citation;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

/**
 * @author madryk
 */
@ExtendWith(MockitoExtension.class)
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
    
    
    @BeforeEach
    public void setup() {
        counterReporter.setReportPath(reportPath);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void report_NULL_SPARK_CONTEXT() {
        // given
        counterReporter.setSparkContext(null);
        
        // execute
        assertThrows(NullPointerException.class, () -> counterReporter.report(matchedCitations));
    }
    
    @Test
    public void report_NULL_REPORT_PATH() {
        // given
        counterReporter.setReportPath(null);
        
        // execute
        assertThrows(NullPointerException.class, () -> counterReporter.report(matchedCitations));
    }
    
    @Test
    public void report() throws Exception {
        
        // given
        
        when(matchedCitations.count()).thenReturn(14L);
        
        doReturn(matchedCitationsDocumentIds).when(matchedCitations).map(any());
        when(matchedCitationsDocumentIds.distinct()).thenReturn(matchedCitationsDistinctDocumentIds);
        when(matchedCitationsDistinctDocumentIds.count()).thenReturn(3L);

        doReturn(reportCounters).when(sparkContext).parallelize(any(), eq(1));


        // execute
        
        counterReporter.report(matchedCitations);
        
        
        // assert
        
        verify(matchedCitations).map(extractDocIdFunction.capture());
        assertExtractDocIdFunction(extractDocIdFunction.getValue());
        
        verify(sparkContext).parallelize(reportEntriesCaptor.capture(), eq(1));
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
        
        assertEquals("processing.citationMatching.citDocReferences.fuzzy", reportEntries.get(0).getKey());
        assertEquals("14", reportEntries.get(0).getValue());
        
        assertEquals("processing.citationMatching.docs.fuzzy", reportEntries.get(1).getKey());
        assertEquals("3", reportEntries.get(1).getValue());
    }
    
}
