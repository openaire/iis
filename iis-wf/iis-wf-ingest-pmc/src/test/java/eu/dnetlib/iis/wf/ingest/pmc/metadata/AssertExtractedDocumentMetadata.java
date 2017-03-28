package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;

import eu.dnetlib.iis.ingest.pmc.metadata.schemas.Affiliation;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.Author;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;

/**
 * Class containing methods for easier asserts of {@link ExtractedDocumentMetadata}
 * elements.
 * 
 * @author madryk
 */
final class AssertExtractedDocumentMetadata {

    
    //------------------------ CONSTRUCTORS --------------------------
    
    private AssertExtractedDocumentMetadata() { }
    
    
    //------------------------ LOGIC --------------------------
    
    public static void assertAuthor(Author actualAuthor, String expectedFullName, Integer ... expectedAffPositions) {
        
        assertEquals(expectedFullName, actualAuthor.getFullname());
        assertThat(actualAuthor.getAffiliationPositions(), containsInAnyOrder(expectedAffPositions));
        
    }
    
    public static void assertAffiliation(Affiliation actualAffiliation, String expectedOrganization, String expectedAddress,
            String expectedCountryCode, String expectedCountryName, String expectedRawText) {
        
        assertEquals(expectedOrganization, actualAffiliation.getOrganization());
        assertEquals(expectedAddress, actualAffiliation.getAddress());
        assertEquals(expectedCountryCode, actualAffiliation.getCountryCode());
        assertEquals(expectedCountryName, actualAffiliation.getCountryName());
        assertEquals(expectedRawText, actualAffiliation.getRawText());
        
    }
}
