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
    
    
    //------------------------ GETTERS --------------------------
    
    /**
     * Returns the match strength of the given voter instance. The match strength must be in the range [0, 1].
     * The match strength equal to 1 says that the match of affiliation and organization (in the {@link #voteMatch(AffMatchAffiliation, AffMatchOrganization)})
     * is 100% reliable, whereas the match strength equal to 0 says that the match is always incorrect. Accordingly, the match strength equal to 0.5 means
     * that the number of correct matches of this voter equals 50%.
     */
    public float getMatchStrength();
    
    
    
}
