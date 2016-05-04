package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.io.Serializable;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * Contract for classes that tell if a given {@link AffMatchAffiliation} and {@link AffMatchOrganization} match
 * according to certain criteria.
 * 
 * @author Łukasz Dumiszewski
*/

public interface AffOrgMatchVoter extends Serializable {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns true if the affiliation and organization match. 
     */
    public boolean voteMatch(AffMatchAffiliation affiliation, AffMatchOrganization organization);
    
}
