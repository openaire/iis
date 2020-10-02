package eu.dnetlib.iis.wf.affmatching.write;

import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author madryk
 */
public class DuplicateMatchedOrgStrengthRecalculatorTest {

    private final static float FLOAT_COMPARE_EPSILON = 0.000001f;
    
    
    private DuplicateMatchedOrgStrengthRecalculator strengthRecalculator = new DuplicateMatchedOrgStrengthRecalculator();
    
    
    private MatchedOrganization matchedOrganization1 = new MatchedOrganization("DOC_ID", "ORG_ID", 0.6f);
    
    private MatchedOrganization matchedOrganization2 = new MatchedOrganization("DOC_ID", "ORG_ID", 0.3f);
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void recalculateStrength_NULL_FIRST_MATCHED_ORG() {
        // execute
        assertThrows(NullPointerException.class, () -> strengthRecalculator.recalculateStrength(null, matchedOrganization2));
    }
    
    @Test
    public void recalculateStrength_NULL_SECOND_MATCHED_ORG() {
        // execute
        assertThrows(NullPointerException.class, () -> strengthRecalculator.recalculateStrength(matchedOrganization1, null));
    }
    
    @Test
    public void recalculateStrength_DIFFERENT_DOC_ID() {
        // given
        matchedOrganization2.setDocumentId("OTHER_DOC_ID");
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> strengthRecalculator.recalculateStrength(matchedOrganization1, matchedOrganization2));
    }
    
    @Test
    public void recalculateStrength_DIFFERENT_ORG_ID() {
        // given
        matchedOrganization2.setOrganizationId("OTHER_ORG_ID");
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> strengthRecalculator.recalculateStrength(matchedOrganization1, matchedOrganization2));
    }
    
    @Test
    public void recalculateStrength() {
        
        // execute
        MatchedOrganization retMatchedOrganization = strengthRecalculator.recalculateStrength(matchedOrganization1, matchedOrganization2);
        
        // assert
        assertEquals("DOC_ID", retMatchedOrganization.getDocumentId());
        assertEquals("ORG_ID", retMatchedOrganization.getOrganizationId());
        assertEquals(0.72f, retMatchedOrganization.getMatchStrength(), FLOAT_COMPARE_EPSILON);
    }
    
}
