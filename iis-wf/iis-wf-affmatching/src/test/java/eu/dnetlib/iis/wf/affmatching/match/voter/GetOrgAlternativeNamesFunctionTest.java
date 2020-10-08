package eu.dnetlib.iis.wf.affmatching.match.voter;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


/**
* @author Łukasz Dumiszewski
*/

public class GetOrgAlternativeNamesFunctionTest {

    private GetOrgAlternativeNamesFunction function = new GetOrgAlternativeNamesFunction();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void apply_NULL() {
        
        // execute
        assertThrows(NullPointerException.class, () -> function.apply(null));
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
