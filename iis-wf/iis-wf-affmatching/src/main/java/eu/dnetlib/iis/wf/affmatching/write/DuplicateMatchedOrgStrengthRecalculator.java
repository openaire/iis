package eu.dnetlib.iis.wf.affmatching.write;

import java.io.Serializable;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;

/**
 * Recalculator of matchStrength of two duplicate {@link MatchedOrganization}s
 * 
 * @author madryk
 */
public class DuplicateMatchedOrgStrengthRecalculator implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Recalculates matchStrength of two duplicated {@link MatchedOrganization}s.<br/>
     * New matchStrength will be calculated according to formula:<br/>
     * <code>matchStrength1 + (1-matchStrength1)*matchStrength2</code><br/>
     * 
     * Method does not modify input {@link MatchedOrganization}s.
     * 
     * @return {@link MatchedOrganization} with recalculated {@link MatchedOrganization#getMatchStrength()}
     */
    public MatchedOrganization recalculateStrength(MatchedOrganization match1, MatchedOrganization match2) {
        
        Preconditions.checkNotNull(match1);
        Preconditions.checkNotNull(match2);
        
        Preconditions.checkArgument(match1.getDocumentId().equals(match2.getDocumentId()));
        Preconditions.checkArgument(match1.getOrganizationId().equals(match2.getOrganizationId()));
        
        
        float remainingMatchStrength = 1 - match1.getMatchStrength();
        
        float newMatchStrength = match1.getMatchStrength() + (remainingMatchStrength * match2.getMatchStrength());
        
        
        return new MatchedOrganization(match1.getDocumentId(), match1.getOrganizationId(), newMatchStrength);
    }
}
