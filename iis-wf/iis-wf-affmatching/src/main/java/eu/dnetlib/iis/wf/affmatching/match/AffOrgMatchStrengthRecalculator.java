package eu.dnetlib.iis.wf.affmatching.match;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVoter;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;

/**
 * Recalculator of {@link AffMatchResult#getMatchStrength()} 
 * 
 * @author Łukasz Dumiszewski
*/

class AffOrgMatchStrengthRecalculator implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(AffOrgMatchStrengthRecalculator.class);
    
    private static final long serialVersionUID = 1L;

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns a new {@link AffMatchResult} with the {@link AffMatchResult#getMatchStrength()} recalculated. The recalculation
     * means here increasing the strength by voterStrength if {@link AffOrgMatchVoter#voteMatch(eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation, eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization)}
     * returns true, or returning unchanged strength otherwise.<br/>
     * Note, the method does not change the passed affMatchResult. 
     */
    public AffMatchResult recalculateMatchStrength(AffMatchResult affMatchResult, AffOrgMatchVoter voter) { 

        Preconditions.checkNotNull(affMatchResult);
        Preconditions.checkNotNull(voter);
        
        float matchStrength = affMatchResult.getMatchStrength();
        
        if (voter.voteMatch(affMatchResult.getAffiliation(), affMatchResult.getOrganization())) {
            if (logger.isTraceEnabled()) {
                logger.trace("voter: '" + voter.toString() + "' voted for the match between \n"
                        + affMatchResult.getAffiliation() + "\nand\n" + affMatchResult.getOrganization());
            }
            
            matchStrength = (matchStrength + voter.getMatchStrength()) - matchStrength * voter.getMatchStrength(); // bear in mind: the strengths are less that one
            
            if (logger.isTraceEnabled()) {
                logger.trace("recalculating current match strength '" + affMatchResult.getMatchStrength()
                        + "' to the new value: '" + matchStrength + "' after getting positive vote from voter '"
                        + voter.toString() + "'");
            }
        } 
        
        return new AffMatchResult(affMatchResult.getAffiliation(), affMatchResult.getOrganization(), matchStrength);
        

    }
    
}
