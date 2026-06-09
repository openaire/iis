package eu.dnetlib.iis.wf.affmatching.write;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        
        
        return new MatchedOrganization(match1.getDocumentId(), combinePositions(match1, match2), match1.getOrganizationId(), newMatchStrength);
    }

    //------------------------ PRIVATE --------------------------

    /**
     * Combines affiliation positions of two {@link MatchedOrganization}s. Resulting list of positions is sorted and does not contain duplicates.
     * @param match1 first {@link MatchedOrganization} to combine positions from
     * @param match2 second {@link MatchedOrganization} to combine positions from
     * @return sorted list of distinct affiliation positions from both {@link MatchedOrganization}s
     */
    private List<Integer> combinePositions(MatchedOrganization match1, MatchedOrganization match2) {
        Stream<Integer> positions1 = match1.getAffiliationPositions() != null ? match1.getAffiliationPositions().stream() : Stream.empty();
        Stream<Integer> positions2 = match2.getAffiliationPositions() != null ? match2.getAffiliationPositions().stream() : Stream.empty();
        return Stream.concat(positions1, positions2)
                     .distinct()
                     .sorted()
                     .collect(Collectors.toList());
    }
}
