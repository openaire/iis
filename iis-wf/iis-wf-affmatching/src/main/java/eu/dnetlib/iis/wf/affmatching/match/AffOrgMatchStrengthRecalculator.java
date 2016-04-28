package eu.dnetlib.iis.wf.affmatching.match;

import java.io.Serializable;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVoter;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;

/**
 * Recalculator of {@link AffMatchResult#getMatchStrength()} 
 * 
 * @author Łukasz Dumiszewski
*/

class AffOrgMatchStrengthRecalculator implements Serializable {

    
    private static final long serialVersionUID = 1L;

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns a new {@link AffMatchResult} with the {@link AffMatchResult#getMatchStrength()} recalculated. The recalculation
     * means here increasing the strength by voterStrength if {@link AffOrgMatchVoter#voteMatch(eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation, eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization)}
     * returns true, or returning unchanged strength otherwise.<br/>
     * Note, the method does not change the passed affMatchResult. 
     */
    public AffMatchResult recalculateMatchStrength(AffMatchResult affMatchResult, AffOrgMatchVoter voter, int voterStrength) { 

        Preconditions.checkNotNull(affMatchResult);
        Preconditions.checkNotNull(voter);
        Preconditions.checkArgument(voterStrength > 0);
        
        float matchStrength = affMatchResult.getMatchStrength();
        
        if (voter.voteMatch(affMatchResult.getAffiliation(), affMatchResult.getOrganization())) {
            
            matchStrength += voterStrength;
            
        } 
        
        return new AffMatchResult(affMatchResult.getAffiliation(), affMatchResult.getOrganization(), matchStrength);
        

    }
    
}
