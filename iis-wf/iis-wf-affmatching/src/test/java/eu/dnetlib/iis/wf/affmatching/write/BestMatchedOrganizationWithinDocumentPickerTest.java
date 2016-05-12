package eu.dnetlib.iis.wf.affmatching.write;

import static com.google.common.collect.ImmutableList.of;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;

/**
 * @author madryk
 */
public class BestMatchedOrganizationWithinDocumentPickerTest {

    private BestMatchedOrganizationWithinDocumentPicker bestMatchedOrganizationPicker = new BestMatchedOrganizationWithinDocumentPicker();
    
    
    private CharSequence documentId = "DOC1";
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void pickBest() {
        
        // given
        
        MatchedOrganization matchedOrganization1 = new MatchedOrganization(documentId, "ORG1", 0.3f);
        MatchedOrganization matchedOrganization2 = new MatchedOrganization(documentId, "ORG1", 0.8f);
        MatchedOrganization matchedOrganization3 = new MatchedOrganization(documentId, "ORG1", 0.5f);
        
        MatchedOrganization matchedOrganization4 = new MatchedOrganization(documentId, "ORG2", 0.8f);
        
        MatchedOrganization matchedOrganization5 = new MatchedOrganization(documentId, "ORG3", 0.9f);
        MatchedOrganization matchedOrganization6 = new MatchedOrganization(documentId, "ORG3", 0.8f);
        
        
        // execute
        
        MatchedOrganization retMatchedOrganization = bestMatchedOrganizationPicker.pickBest(of(
                matchedOrganization1, matchedOrganization2, matchedOrganization3, 
                matchedOrganization4, matchedOrganization5, matchedOrganization6));
        
        
        // assert
        
        assertEquals(matchedOrganization2, retMatchedOrganization);
        
    }
}
