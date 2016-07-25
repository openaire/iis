package eu.dnetlib.iis.wf.affmatching.match.voter;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;


/**
* @author Łukasz Dumiszewski
*/

public class GetOrgAlternativeNamesFunctionTest {

    private GetOrgAlternativeNamesFunction function = new GetOrgAlternativeNamesFunction();
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected=NullPointerException.class)
    public void apply_NULL() {
        
        // execute
        function.apply(null);
    }
    
    
    @Test
    public void apply() {
        
        // given
        AffMatchOrganization organization = new AffMatchOrganization("ORG1");
        organization.addAlternativeName("Mickey Mouse's Cheese Factory");
        organization.addAlternativeName("Fabryka Sera Myszki Miki");
        
        // execute
        List<String> names = function.apply(organization);
        
        // assert
        assertEquals(organization.getAlternativeNames().size(), names.size());
        assertEquals(names, organization.getAlternativeNames());
    }
    
}
