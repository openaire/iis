package eu.dnetlib.iis.wf.affmatching.write;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
* @author Łukasz Dumiszewski
*/

public class AffMatchResultConverterTest {

    private AffMatchResultConverter converter = new AffMatchResultConverter();
    
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test
    public void convert_null() {
        
        // execute
        assertThrows(NullPointerException.class, () -> converter.convert(null));
        
    }
    

    @Test
    public void convert() {
        
        // given
        
        AffMatchAffiliation aff = new AffMatchAffiliation("DOC1", 1);
        AffMatchOrganization org = new AffMatchOrganization("ORG1");
        AffMatchResult affMatchResult = new AffMatchResult(aff, org, 0.85f);
        
        
        // execute
        
        MatchedOrganization matchedOrg = converter.convert(affMatchResult);
        
        
        // assert
        
        assertNotNull(matchedOrg);
        assertEquals("DOC1", matchedOrg.getDocumentId());
        assertEquals("ORG1", matchedOrg.getOrganizationId());
        assertEquals(0.85f, matchedOrg.getMatchStrength(), 0.002);
        
    }

    
    
}
