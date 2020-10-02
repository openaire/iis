package eu.dnetlib.iis.wf.affmatching.match.voter;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


/**
* @author Łukasz Dumiszewski
*/

public class GetOrgShortNameFunctionTest {

    private GetOrgShortNameFunction function = new GetOrgShortNameFunction();
    
    
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
        organization.setShortName("MMCF");
        
        // execute
        List<String> names = function.apply(organization);
        
        // assert
        assertEquals(1, names.size());
        assertEquals(organization.getShortName(), names.get(0));
    }
    
}
