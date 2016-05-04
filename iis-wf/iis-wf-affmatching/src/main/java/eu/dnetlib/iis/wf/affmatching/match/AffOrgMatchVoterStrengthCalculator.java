package eu.dnetlib.iis.wf.affmatching.match;

import java.io.Serializable;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVoter;

/**
 * {@link AffOrgMatchVoter} strength calculator.
 * 
 * @author Łukasz Dumiszewski
*/

class AffOrgMatchVoterStrengthCalculator implements Serializable {

    
    private static final long serialVersionUID = 1L;

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Calculates the match strength of the {link AffOrgMatchVoter} on its position on the list
     * of used voters. 
     * @param voterPosition must be greater or equal to 0 (0 for the first position), voterPosition must also be less than numberOfVoters
     * @param numberOfVoters must be greater than 0
     */
    public int calculateStrength(int voterPosition, int numberOfVoters) {
        
        Preconditions.checkArgument(voterPosition >= 0);
        Preconditions.checkArgument(numberOfVoters > 0);
        Preconditions.checkArgument(voterPosition < numberOfVoters);
        
        return (int)Math.pow(2, numberOfVoters-(voterPosition+1));
        
    }
    
}
