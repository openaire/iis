package eu.dnetlib.iis.wf.affmatching.write;

import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author madryk
 */
public class DuplicateMatchedOrgStrengthRecalculatorTest {

    private final static float FLOAT_COMPARE_EPSILON = 0.000001f;
    
    
    private DuplicateMatchedOrgStrengthRecalculator strengthRecalculator = new DuplicateMatchedOrgStrengthRecalculator();
    
    private List<Integer> positions1 = Collections.singletonList(1);

    private List<Integer> positions2 = Collections.singletonList(2);

    private MatchedOrganization matchedOrganization1 = new MatchedOrganization("DOC_ID", positions1, "ORG_ID", 0.6f);
    
    private MatchedOrganization matchedOrganization2 = new MatchedOrganization("DOC_ID", positions2, "ORG_ID", 0.3f);
    
    
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
        assertEquals(Arrays.asList(1, 2), retMatchedOrganization.getAffiliationPositions());
        assertEquals(0.72f, retMatchedOrganization.getMatchStrength(), FLOAT_COMPARE_EPSILON);
    }
    
    @Test
    public void recalculateStrength_NULL_AFFILIATION_POSITIONS() {
        
        // given
        matchedOrganization1.setAffiliationPositions(null);
        matchedOrganization2.setAffiliationPositions(null);        
        // execute
        MatchedOrganization retMatchedOrganization = strengthRecalculator.recalculateStrength(matchedOrganization1, matchedOrganization2);
        
        // assert
        assertEquals("DOC_ID", retMatchedOrganization.getDocumentId());
        assertEquals("ORG_ID", retMatchedOrganization.getOrganizationId());
        assertEquals(Collections.emptyList(), retMatchedOrganization.getAffiliationPositions());
        assertEquals(0.72f, retMatchedOrganization.getMatchStrength(), FLOAT_COMPARE_EPSILON);
    }
}
