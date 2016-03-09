package eu.dnetlib.iis.wf.citationmatching.direct.converter;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.collect.Maps;

import eu.dnetlib.iis.common.citations.schemas.Citation;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;

/**
 * 
 * @author madryk
 *
 */
public class DirectCitationToCitationConverterTest {

    private DirectCitationToCitationConverter converter = new DirectCitationToCitationConverter();
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void convert_NULL() {
        // execute
        converter.convert(null);
    }
    
    @Test
    public void convert() {
        
        // given
        eu.dnetlib.iis.citationmatching.direct.schemas.Citation directCitation = eu.dnetlib.iis.citationmatching.direct.schemas.Citation.newBuilder()
                .setSourceDocumentId("source-doc-id")
                .setPosition(4)
                .setDestinationDocumentId("dest-doc-id")
                .build();
        
        
        // execute
        Citation retCitation = converter.convert(directCitation);
        
        
        // assert
        Citation expectedCitation = Citation.newBuilder()
                .setSourceDocumentId("source-doc-id")
                .setEntry(CitationEntry.newBuilder()
                        .setConfidenceLevel(1f)
                        .setDestinationDocumentId("dest-doc-id")
                        .setExternalDestinationDocumentIds(Maps.newHashMap())
                        .setPosition(4)
                        .setRawText(null)
                        .build())
                .build();
        
        assertEquals(expectedCitation, retCitation);
        
    }
}
