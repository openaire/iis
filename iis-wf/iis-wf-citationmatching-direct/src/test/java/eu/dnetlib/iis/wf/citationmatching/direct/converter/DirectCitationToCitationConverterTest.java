package eu.dnetlib.iis.wf.citationmatching.direct.converter;

import com.google.common.collect.Maps;
import eu.dnetlib.iis.common.citations.schemas.Citation;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 
 * @author madryk
 *
 */
public class DirectCitationToCitationConverterTest {

    private DirectCitationToCitationConverter converter = new DirectCitationToCitationConverter();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void convert_NULL() {
        // execute
        assertThrows(NullPointerException.class, () -> converter.convert(null));
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
