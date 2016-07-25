package eu.dnetlib.iis.wf.affmatching.match.voter;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;


/**
* @author Łukasz Dumiszewski
*/

public class GetOrgNameFunctionTest {

    private GetOrgNameFunction function = new GetOrgNameFunction();
    
    
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
        organization.setName("Mickey Mouse's Cheese Factory");
        
        // execute
        List<String> names = function.apply(organization);
        
        // assert
        assertEquals(1, names.size());
        assertEquals(organization.getName(), names.get(0));
    }
    
}
