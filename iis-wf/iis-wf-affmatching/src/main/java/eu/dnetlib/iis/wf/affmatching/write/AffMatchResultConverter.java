package eu.dnetlib.iis.wf.affmatching.write;

import java.io.Serializable;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import eu.dnetlib.iis.wf.affmatching.model.MatchedAffiliation;

/**
 * Converter of {@link AffMatchResult} into {@link MatchedAffiliation}. 
 * 
 * @author Łukasz Dumiszewski
*/

public class AffMatchResultConverter implements Serializable {

    
    private static final long serialVersionUID = 1L;

    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Converts {@link AffMatchResult} into {@link MatchedAffiliation} 
     */
    public MatchedAffiliation convert(AffMatchResult affMatchResult) {
        
        Preconditions.checkNotNull(affMatchResult);
        
        MatchedAffiliation matchedAffiliation = new MatchedAffiliation();
        
        matchedAffiliation.setDocumentId(affMatchResult.getAffiliation().getDocumentId());
        
        matchedAffiliation.setOrganizationId(affMatchResult.getOrganization().getId());
        
        matchedAffiliation.setMatchStrength(affMatchResult.getMatchStrength());
        
        return matchedAffiliation;
    }
    
    
}
