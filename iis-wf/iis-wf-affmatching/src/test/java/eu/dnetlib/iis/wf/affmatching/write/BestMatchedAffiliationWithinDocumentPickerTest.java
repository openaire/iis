package eu.dnetlib.iis.wf.affmatching.write;

import static com.google.common.collect.ImmutableList.of;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.dnetlib.iis.wf.affmatching.model.MatchedAffiliation;

/**
 * @author madryk
 */
public class BestMatchedAffiliationWithinDocumentPickerTest {

    private BestMatchedAffiliationWithinDocumentPicker bestMatchedAffiliationPicker = new BestMatchedAffiliationWithinDocumentPicker();
    
    
    private CharSequence documentId = "DOC1";
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void pickBest() {
        
        // given
        
        MatchedAffiliation matchedAffiliation1 = new MatchedAffiliation(documentId, "ORG1", 0.3f);
        MatchedAffiliation matchedAffiliation2 = new MatchedAffiliation(documentId, "ORG1", 0.8f);
        MatchedAffiliation matchedAffiliation3 = new MatchedAffiliation(documentId, "ORG1", 0.5f);
        
        MatchedAffiliation matchedAffiliation4 = new MatchedAffiliation(documentId, "ORG2", 0.8f);
        
        MatchedAffiliation matchedAffiliation5 = new MatchedAffiliation(documentId, "ORG3", 0.9f);
        MatchedAffiliation matchedAffiliation6 = new MatchedAffiliation(documentId, "ORG3", 0.8f);
        
        
        // execute
        
        MatchedAffiliation retMatchedAffiliation = bestMatchedAffiliationPicker.pickBest(of(
                matchedAffiliation1, matchedAffiliation2, matchedAffiliation3, 
                matchedAffiliation4, matchedAffiliation5, matchedAffiliation6));
        
        
        // assert
        
        assertEquals(matchedAffiliation2, retMatchedAffiliation);
        
    }
}
