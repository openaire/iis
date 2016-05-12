package eu.dnetlib.iis.wf.affmatching.write;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
import scala.Tuple2;

/**
 * Picker of best matched organization within single document
 * 
 * @author madryk
 */
public class BestMatchedOrganizationWithinDocumentPicker implements Serializable {

    private static final long serialVersionUID = 1L;


    //------------------------ LOGIC --------------------------
    
    /**
     * Picks best matched organization from matched affiliations within single document.<br/>
     * 
     * Matched organization is considered the best if its {@link MatchedOrganization#getOrganizationId()}
     * appeared in all matches more commonly then other organization ids and its
     * {@link MatchedOrganization#getMatchStrength()} was highest.
     */
    public MatchedOrganization pickBest(Iterable<MatchedOrganization> matchedOrganizations) {
        
        Collection<List<MatchedOrganization>> matchedGroupedByOrgId = groupByOrganizationId(matchedOrganizations);
        
        return matchedGroupedByOrgId.stream()
            .map(matchesWithSameOrg -> new Tuple2<>(matchesWithSameOrg.size(), fetchMatchWithMaxStrength(matchesWithSameOrg)))
            .reduce((match1, match2) -> pickBetterMatch(match1, match2))
            .get()._2;
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private Collection<List<MatchedOrganization>> groupByOrganizationId(Iterable<MatchedOrganization> matchedOrganizations) {
        Map<CharSequence, List<MatchedOrganization>> matchedGroupedByOrgId = Maps.newHashMap();
        
        for (MatchedOrganization matched : matchedOrganizations) {
            
            if (matchedGroupedByOrgId.containsKey(matched.getOrganizationId())) {
                matchedGroupedByOrgId.get(matched.getOrganizationId()).add(matched);
            } else {
                matchedGroupedByOrgId.put(matched.getOrganizationId(), Lists.newArrayList(matched));
            }
            
        }
        
        return matchedGroupedByOrgId.values();
    }
    
    private Tuple2<Integer, MatchedOrganization> pickBetterMatch(Tuple2<Integer, MatchedOrganization> first, Tuple2<Integer, MatchedOrganization> second) {
        
        if (first._1 > second._1) {
            return first;
        } else if (first._1 == second._1) {
            return (first._2.getMatchStrength() >= second._2.getMatchStrength()) ? first : second;
        }
        return second;
    }
    
    private MatchedOrganization fetchMatchWithMaxStrength(List<MatchedOrganization> matched) {
        
        return matched.stream().max((match1, match2) -> match1.getMatchStrength().compareTo(match2.getMatchStrength())).get();
        
    }
}
