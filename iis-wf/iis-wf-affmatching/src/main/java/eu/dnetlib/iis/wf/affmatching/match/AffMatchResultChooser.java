package eu.dnetlib.iis.wf.affmatching.match;

import java.io.Serializable;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;

/**
 *
 * Chooser of one from the two given {@link AffMatchResult}s 
 * 
 * @author Łukasz Dumiszewski
*/

public class AffMatchResultChooser implements Serializable {

    
    private static final long serialVersionUID = 1L;
    
    private static final float EPSILON = 0.0000001f;

    
    
    //------------------------ LOGIC --------------------------
    
    
    /**
     * Returns the 'better' {@link AffMatchResult} from the passed two. 'Better' means here the one that has
     * bigger {@link AffMatchResult#getMatchStrength()}.
     */
    public AffMatchResult chooseBetter(AffMatchResult affMatchResult1, AffMatchResult affMatchResult2) {
        
        Preconditions.checkNotNull(affMatchResult1);
        Preconditions.checkNotNull(affMatchResult2);
        
        // we want the results to be repetitive and independent of float rounding
        if (Math.abs(affMatchResult1.getMatchStrength() - affMatchResult2.getMatchStrength()) < EPSILON) {
            return affMatchResult1;
        }
        
        if (affMatchResult1.getMatchStrength() > affMatchResult2.getMatchStrength()) {
            return affMatchResult1;
        }    
        
        
        return affMatchResult2;
        
        
    }

}
