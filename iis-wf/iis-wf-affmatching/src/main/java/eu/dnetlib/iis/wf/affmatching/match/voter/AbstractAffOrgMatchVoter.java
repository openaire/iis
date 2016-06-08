package eu.dnetlib.iis.wf.affmatching.match.voter;

import com.google.common.base.Preconditions;

/**
* @author Łukasz Dumiszewski
*/

public abstract class AbstractAffOrgMatchVoter implements AffOrgMatchVoter {

    private static final long serialVersionUID = 1L;

    private float matchStrength;
    
    
    
    //------------------------ GETTERS --------------------------
    
    @Override
    public float getMatchStrength() {
        return matchStrength;
    }
    
    
    //------------------------ SETTERS --------------------------
    
    /**
     * Sets the match strength of the given voter instance. The match strength must be in the range [0, 1]
     * @see #getMatchStrength()
     */
    public void setMatchStrength(float matchStrength) {
        
        Preconditions.checkArgument(matchStrength >=0 && matchStrength <=1);
        
        this.matchStrength = matchStrength;
    
    }
}
