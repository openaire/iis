package eu.dnetlib.iis.wf.affmatching.write;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import eu.dnetlib.iis.wf.affmatching.model.MatchedAffiliation;

/**
* @author Łukasz Dumiszewski
*/

public class AffMatchResultConverterTest {

    private AffMatchResultConverter converter = new AffMatchResultConverter();
    
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test(expected = NullPointerException.class)
    public void convert_null() {
        
        // execute
        
        converter.convert(null);
        
    }
    

    @Test
    public void convert() {
        
        // given
        
        AffMatchAffiliation aff = new AffMatchAffiliation("DOC1", 1);
        AffMatchOrganization org = new AffMatchOrganization("ORG1");
        AffMatchResult affMatchResult = new AffMatchResult(aff, org, 0.85f);
        
        
        // execute
        
        MatchedAffiliation matchedAff = converter.convert(affMatchResult);
        
        
        // assert
        
        assertNotNull(matchedAff);
        assertEquals("DOC1", matchedAff.getDocumentId());
        assertEquals(1, matchedAff.getPosition().intValue());
        assertEquals("ORG1", matchedAff.getOrganizationId());
        assertEquals(0.85f, matchedAff.getMatchStrength().floatValue(), 0.002);
        
    }

    
    
}
