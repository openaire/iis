package eu.dnetlib.iis.wf.affmatching.write;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import eu.dnetlib.iis.wf.affmatching.model.MatchedAffiliation;
import scala.Tuple2;

/**
 * Picker of best matched affiliation within single document
 * 
 * @author madryk
 */
public class BestMatchedAffiliationWithinDocumentPicker implements Serializable {

    private static final long serialVersionUID = 1L;


    //------------------------ LOGIC --------------------------
    
    /**
     * Picks best matched affiliation from matched affiliations within single document.<br/>
     * 
     * Matched affiliation is considered the best if its {@link MatchedAffiliation#getOrganizationId()}
     * appeared in all matches more commonly then other organization ids and its
     * {@link MatchedAffiliation#getMatchStrength()} was highest.
     */
    public MatchedAffiliation pickBest(Iterable<MatchedAffiliation> matchedAffs) {
        
        Collection<List<MatchedAffiliation>> matchedGroupedByOrgId = groupByOrganizationId(matchedAffs);
        
        return matchedGroupedByOrgId.stream()
            .map(matchesWithSameOrg -> new Tuple2<>(matchesWithSameOrg.size(), fetchMatchWithMaxStrength(matchesWithSameOrg)))
            .reduce((match1, match2) -> pickBetterMatch(match1, match2))
            .get()._2;
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private Collection<List<MatchedAffiliation>> groupByOrganizationId(Iterable<MatchedAffiliation> matchedAffs) {
        Map<CharSequence, List<MatchedAffiliation>> matchedGroupedByOrgId = Maps.newHashMap();
        
        for (MatchedAffiliation matched : matchedAffs) {
            
            if (matchedGroupedByOrgId.containsKey(matched.getOrganizationId())) {
                matchedGroupedByOrgId.get(matched.getOrganizationId()).add(matched);
            } else {
                matchedGroupedByOrgId.put(matched.getOrganizationId(), Lists.newArrayList(matched));
            }
            
        }
        
        return matchedGroupedByOrgId.values();
    }
    
    private Tuple2<Integer, MatchedAffiliation> pickBetterMatch(Tuple2<Integer, MatchedAffiliation> first, Tuple2<Integer, MatchedAffiliation> second) {
        
        if (first._1 > second._1) {
            return first;
        } else if (first._1 == second._1) {
            return (first._2.getMatchStrength() >= second._2.getMatchStrength()) ? first : second;
        }
        return second;
    }
    
    private MatchedAffiliation fetchMatchWithMaxStrength(List<MatchedAffiliation> matched) {
        
        return matched.stream().max((match1, match2) -> match1.getMatchStrength().compareTo(match2.getMatchStrength())).get();
        
    }
}
