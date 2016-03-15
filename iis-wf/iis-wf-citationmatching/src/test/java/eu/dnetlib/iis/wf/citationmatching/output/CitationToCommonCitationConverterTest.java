package eu.dnetlib.iis.wf.citationmatching.output;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.collect.Maps;

import eu.dnetlib.iis.common.citations.schemas.Citation;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;

/**
 * @author madryk
 */
public class CitationToCommonCitationConverterTest {

    private CitationToCommonCitationConverter converter = new CitationToCommonCitationConverter();
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void convert_NULL_CITATION() {
        
        // execute
        converter.convert(null);
    }
    
    @Test
    public void convert() {
        
        // given
        
        eu.dnetlib.iis.citationmatching.schemas.Citation inputCitation = eu.dnetlib.iis.citationmatching.schemas.Citation.newBuilder()
                .setSourceDocumentId("source-id")
                .setDestinationDocumentId("dest-id")
                .setPosition(4)
                .setConfidenceLevel(0.73f)
                .build();
        
        
        // execute
        
        Citation retCitation = converter.convert(inputCitation);
        
        
        // assert
        
        Citation expectedCitation = Citation.newBuilder()
                .setSourceDocumentId("source-id")
                .setEntry(CitationEntry.newBuilder()
                        .setPosition(4)
                        .setDestinationDocumentId("dest-id")
                        .setConfidenceLevel(0.73f)
                        .setRawText(null)
                        .setExternalDestinationDocumentIds(Maps.newHashMap())
                        .build())
                .build();
        
        assertEquals(expectedCitation, retCitation);
        
    }
    
    
}
