package eu.dnetlib.iis.wf.affmatching.write;

import java.io.Serializable;
import java.util.Collections;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;

/**
 * Converter of {@link AffMatchResult} into {@link MatchedAffiliation}. 
 * 
 * @author Łukasz Dumiszewski
*/

public class AffMatchResultConverter implements Serializable {

    
    private static final long serialVersionUID = 1L;

    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Converts {@link AffMatchResult} into {@link MatchedOrganization} 
     */
    public MatchedOrganization convert(AffMatchResult affMatchResult) {
        
        Preconditions.checkNotNull(affMatchResult);
        
        MatchedOrganization matchedOrganization = new MatchedOrganization();
        
        matchedOrganization.setDocumentId(affMatchResult.getAffiliation().getDocumentId());

        matchedOrganization.setAffiliationPositions(Collections.singletonList(affMatchResult.getAffiliation().getPosition()));
        
        matchedOrganization.setOrganizationId(affMatchResult.getOrganization().getId());
        
        matchedOrganization.setMatchStrength(affMatchResult.getMatchStrength());
        
        return matchedOrganization;
    }
    
    
}
