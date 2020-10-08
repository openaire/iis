package eu.dnetlib.iis.wf.affmatching.write;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static eu.dnetlib.iis.common.report.ReportEntryFactory.createCounterReportEntry;
import static eu.dnetlib.iis.wf.affmatching.write.AffMatchReportCounters.DOCS_WITH_AT_LEAST_ONE_ORG;
import static eu.dnetlib.iis.wf.affmatching.write.AffMatchReportCounters.DOC_ORG_REFERENCES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;


/**
* @author Łukasz Dumiszewski
*/
@ExtendWith(MockitoExtension.class)
public class AffMatchReportGeneratorTest {

    private AffMatchReportGenerator reportGenerator = new AffMatchReportGenerator();
    
    @Mock
    private JavaRDD<MatchedOrganization> matchedDocOrganizations;
    
    @Mock
    private JavaRDD<CharSequence> docIds;
    
    @Mock
    private JavaRDD<CharSequence> distinctDocIds;
    
    @Captor
    private ArgumentCaptor<Function<MatchedOrganization, CharSequence>> mapToDocIdFunction;
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void generateReport_MatchedDocOrganizations_NULL() {
        
        // execute
        assertThrows(NullPointerException.class, () -> reportGenerator.generateReport(null));

    }
    
    @Test
    public void generateReport() throws Exception {
        
        
        // given
        
        when(matchedDocOrganizations.count()).thenReturn(199L);
        doReturn(docIds).when(matchedDocOrganizations).map(Mockito.any());
        when(docIds.distinct()).thenReturn(distinctDocIds);
        when(distinctDocIds.count()).thenReturn(88L);
        
        
        // execute
        
        List<ReportEntry> reportEntries = reportGenerator.generateReport(matchedDocOrganizations);
        
        
        // assert
        
        assertThat(reportEntries, containsInAnyOrder(createCounterReportEntry(DOC_ORG_REFERENCES, 199),
                                                     createCounterReportEntry(DOCS_WITH_AT_LEAST_ONE_ORG, 88)));
        
        verify(matchedDocOrganizations).map(mapToDocIdFunction.capture());
        
        assertMapToDocIdFunction(mapToDocIdFunction.getValue());
        
    }

    
    //------------------------ PRIVATE --------------------------
    
    private void assertMapToDocIdFunction(Function<MatchedOrganization, CharSequence> function) throws Exception {
        
        // given
        
        MatchedOrganization mOrg = mock(MatchedOrganization.class);
        when(mOrg.getDocumentId()).thenReturn("XYZ");
        
        // execute
        
        CharSequence docId = function.call(mOrg);
        
        // assert
        assertEquals("XYZ", docId.toString());
        
    }
    
    
    
    
    
    
    
    
}
