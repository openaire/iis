package eu.dnetlib.iis.wf.collapsers.basic;

import eu.dnetlib.iis.common.citations.schemas.Citation;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
import eu.dnetlib.iis.wf.collapsers.basic.GenericCitationCollapser.CitationTextCounters;
import eu.dnetlib.iis.wf.collapsers.basic.GenericCitationCollapser.MatchedCitationCounters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author mhorst
 *
 */
@ExtendWith(MockitoExtension.class)
public class GenericCitationCollapserTest {

    @Mock
    private TaskAttemptContext context;
    
    @Mock
    private Counter totalTextCounter;
    
    @Mock
    private Counter docsWithAtLeastOneCitationTextCounter;
    
    @Mock
    private Counter totalMatchedCounter;
    
    @Mock
    private Counter docsWithAtLeastOneMatchedCitationCounter;
    
    
    private GenericCitationCollapser collapser = new GenericCitationCollapser();
    
    
    @BeforeEach
    public void prepareCounters() {
        doReturn(totalTextCounter).when(context).getCounter(CitationTextCounters.TOTAL);
        doReturn(docsWithAtLeastOneCitationTextCounter).when(context).getCounter(CitationTextCounters.DOCS_WITH_AT_LEAST_ONE_CITATION);
        doReturn(totalMatchedCounter).when(context).getCounter(MatchedCitationCounters.TOTAL);
        doReturn(docsWithAtLeastOneMatchedCitationCounter).when(context).getCounter(MatchedCitationCounters.DOCS_WITH_AT_LEAST_ONE_CITATION);
    }
    
    
    // --------------------------------- TEST ------------------------------------
    
    @Test
    public void testSetup() throws Exception {
        // execute
        collapser.setup(context);
        
        // assert
        verify(totalTextCounter).setValue(0);
        verify(docsWithAtLeastOneCitationTextCounter).setValue(0);
    }
    
    @Test
    public void testCollapseNull() throws Exception {
        // given
        collapser.setup(context);
        
        // execute
        List<Citation> result = collapser.collapse(null);
        
        // assert
        assertNull(result);
    }
    
    @Test
    public void testCollapseEmpty() throws Exception {
        // given
        collapser.setup(context);
        
        // execute
        List<Citation> result = collapser.collapse(Collections.emptyList());
        
        // assert
        assertNull(result);
    }
    
    @Test
    public void testCollapseEmptyTextOnly() throws Exception {
     // given
        collapser.setup(context);
        
        List<Citation> source = new ArrayList<>();
        
        String citationText = "";
        source.add(buildCitationWithText(1, citationText));
        // execute
        List<Citation> result = collapser.collapse(source);
        
        // assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getEntry().getPosition().intValue());
        assertEquals(citationText, result.get(0).getEntry().getRawText());
        
        verify(totalTextCounter, never()).increment(1);
        verify(docsWithAtLeastOneCitationTextCounter, never()).increment(1);
        verify(totalMatchedCounter, never()).increment(1);
        verify(docsWithAtLeastOneMatchedCitationCounter, never()).increment(1);
    }
    
    @Test
    public void testCollapseTextOnly() throws Exception {
     // given
        collapser.setup(context);
        
        List<Citation> source = new ArrayList<>();
        
        String citation1Text = "citation text";
        String citation1OtherText = "citation new text";
        String citation2Text = "some other text";
        source.add(buildCitationWithText(1, citation1Text));
        source.add(buildCitationWithText(1, citation1OtherText));
        source.add(buildCitationWithText(2, citation2Text));

        // execute
        List<Citation> result = collapser.collapse(source);
        
        // assert
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals(1, result.get(0).getEntry().getPosition().intValue());
        assertEquals(citation1OtherText, result.get(0).getEntry().getRawText());
        assertEquals(2, result.get(1).getEntry().getPosition().intValue());
        assertEquals(citation2Text, result.get(1).getEntry().getRawText());
        
        verify(totalTextCounter, times(1)).increment(2);
        verify(docsWithAtLeastOneCitationTextCounter, times(1)).increment(1);
        verify(totalMatchedCounter, never()).increment(1);
        verify(docsWithAtLeastOneMatchedCitationCounter, never()).increment(1);
    }
    
    @Test
    public void testCollapseMultipleFields() throws Exception {
        // given
        collapser.setup(context);
        
        String citationText = "citation text";
        String destId = "destId";
        List<Citation> source = new ArrayList<>();
        source.add(buildCitationWithText(1, citationText));
        
        source.add(buildCitationWithDestId(1, destId, 0.8f));
        
        String doiType = "doi";
        String doiValue = "1234";
        Map<CharSequence, CharSequence> externalIds1 = new HashMap<>();
        externalIds1.put(doiType, doiValue);
        source.add(buildCitationWithExternalIds(1, externalIds1, null));
        
        String pmidType = "pmid";
        String pmidValue = "6789";
        String doiValue2 = "12345";
        Map<CharSequence, CharSequence> externalIds2 = new HashMap<>();
        externalIds2.put(pmidType, pmidValue);
        externalIds2.put(doiType, doiValue2);
        source.add(buildCitationWithExternalIds(1, externalIds2, 0.7f));
        
        // execute
        List<Citation> result = collapser.collapse(source);
        
        // assert
        assertNotNull(result);
        assertEquals(1, result.size());
        
        assertEquals(1, result.get(0).getEntry().getPosition().intValue());
        assertEquals(citationText, result.get(0).getEntry().getRawText());
        assertEquals(destId, result.get(0).getEntry().getDestinationDocumentId());
        assertEquals(2, result.get(0).getEntry().getExternalDestinationDocumentIds().size());
        assertEquals(doiValue2, result.get(0).getEntry().getExternalDestinationDocumentIds().get(doiType));
        assertEquals(pmidValue, result.get(0).getEntry().getExternalDestinationDocumentIds().get(pmidType));
        
        assertEquals(0.8f, result.get(0).getEntry().getConfidenceLevel().floatValue(), 0.0001);
        
        verify(totalTextCounter, times(1)).increment(1);
        verify(docsWithAtLeastOneCitationTextCounter, times(1)).increment(1);
        verify(totalMatchedCounter, times(1)).increment(1);
        verify(docsWithAtLeastOneMatchedCitationCounter, times(1)).increment(1);

    }
    
    // --------------------------------- PRIVATE ------------------------------------

    private static final Citation buildCitationWithText(
            int position, String text) {
        return buildCitation(position, text, null, new HashMap<>(), 0.5f);
    }
    
    private static final Citation buildCitationWithDestId(
            int position, String destDocId, Float confidenceLevel) {
        return buildCitation(position, null, destDocId, new HashMap<>(), confidenceLevel);
    }
    
    private static final Citation buildCitationWithExternalIds(
            int position, Map<CharSequence, CharSequence> externalIds, Float confidenceLevel) {
        return buildCitation(position, null, null, externalIds, confidenceLevel);
    }
    
    private static final Citation buildCitation(int position, String text, 
            String destDocId, Map<CharSequence, CharSequence> externalIds, Float confidenceLevel) {
        Citation.Builder citationBuilder = Citation.newBuilder();
        citationBuilder.setSourceDocumentId("sourceId");
        CitationEntry.Builder citationEntryBuilder = CitationEntry.newBuilder();
        citationEntryBuilder.setPosition(position);
        if (text != null) {
            citationEntryBuilder.setRawText(text);    
        }
        if (destDocId != null) {
            citationEntryBuilder.setDestinationDocumentId(destDocId);    
        }
        if (externalIds != null) {
            citationEntryBuilder.setExternalDestinationDocumentIds(externalIds);    
        }
        if (confidenceLevel != null) {
            citationEntryBuilder.setConfidenceLevel(confidenceLevel);    
        }
        citationBuilder.setEntry(citationEntryBuilder.build());
        return citationBuilder.build();
    }
    
}
