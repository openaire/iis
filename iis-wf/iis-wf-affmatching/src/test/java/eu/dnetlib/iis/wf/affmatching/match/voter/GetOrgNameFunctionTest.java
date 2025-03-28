package eu.dnetlib.iis.wf.affmatching.match.voter;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


/**
* @author Łukasz Dumiszewski
*/

public class GetOrgNameFunctionTest {

    private GetOrgNameFunction function = new GetOrgNameFunction();
    
    
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
        organization.setName("Mickey Mouse's Cheese Factory");
        
        // execute
        List<String> names = function.apply(organization);
        
        // assert
        assertEquals(1, names.size());
        assertEquals(organization.getName(), names.get(0));
    }
    
}
