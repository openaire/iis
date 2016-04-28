package eu.dnetlib.iis.wf.affmatching.match.voter;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
* @author Łukasz Dumiszewski
*/

public class NameCountryStrictMatchVoter implements AffOrgMatchVoter {

    
    private static final long serialVersionUID = 1L;

    
    
    //------------------------ LOGIC --------------------------
    
    
    /**
     * Returns true if the name of the passed organization is the same as the name of the organization in the passed affiliation
     * AND if the country codes of the passed organization and affiliation are also the same. 
     */
    @Override
    public boolean voteMatch(AffMatchAffiliation affiliation, AffMatchOrganization organization) {
        
        return affiliation.getOrganizationName().equals(organization.getName()) &&
               affiliation.getCountryCode().equals(organization.getCountryCode());
    }

    
    
    
}
