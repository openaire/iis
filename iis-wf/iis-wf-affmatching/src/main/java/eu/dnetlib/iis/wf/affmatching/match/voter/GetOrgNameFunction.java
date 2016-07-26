package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;


/**
 * Function that returns {@link AffMatchOrganization#getName()} of the passed organization.
 * 
* @author Łukasz Dumiszewski
*/

public class GetOrgNameFunction implements Function<AffMatchOrganization, List<String>>, Serializable {

    
    private static final long serialVersionUID = 1L;

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns an immutable list that contains only one element - {@link AffMatchOrganization#getName()} of the passed organization.
     */
    @Override
    public List<String> apply(AffMatchOrganization organization) {
        
        Preconditions.checkNotNull(organization);
        
        return ImmutableList.of(organization.getName());
    }

}
