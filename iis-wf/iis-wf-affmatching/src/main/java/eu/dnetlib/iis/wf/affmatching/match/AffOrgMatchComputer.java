package eu.dnetlib.iis.wf.affmatching.match;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.JavaRDD;

import eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVoter;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import scala.Tuple2;

/**
 *
 * Service that computes the match strength of pairs of {@link AffMatchAffiliation} and {@link AffMatchOrganization}
 * 
 * 
 * @author Łukasz Dumiszewski
*/

public class AffOrgMatchComputer implements Serializable {

    
    private static final long serialVersionUID = 1L;

    
    private List<AffOrgMatchVoter> affOrgMatchVoters;
    
    private AffOrgMatchStrengthRecalculator affOrgMatchStrengthRecalculator = new AffOrgMatchStrengthRecalculator();
    
    
    
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Computes the match strength of every passed pair of affiliation and organization. The returned strengths are in the range
     * of (0, 1]. The pairs which have the match strength equal to 0 are filtered out and not returned.<br/><br/>
     * The match strength of the given affiliation-organization pair depends on how many of the {@link #setAffOrgMatchVoters(List)}
     * say the pair matches.
     */
    public JavaRDD<AffMatchResult> computeMatches(JavaRDD<Tuple2<AffMatchAffiliation, AffMatchOrganization>> joinedAffOrgs) {
        
        checkNotNull(joinedAffOrgs);
    
        checkState(CollectionUtils.isNotEmpty(affOrgMatchVoters), "no AffOrgMatchVoter has been set");
        
        
        JavaRDD<AffMatchResult> affMatchResults = joinedAffOrgs.map(kv->new AffMatchResult(kv._1(), kv._2(), 0));
        
        
        for (AffOrgMatchVoter voter : affOrgMatchVoters) {
            
            affMatchResults = affMatchResults.map(affOrgMatch ->  affOrgMatchStrengthRecalculator.recalculateMatchStrength(affOrgMatch, voter));
            
        }
        
        affMatchResults = affMatchResults.filter(affOrgMatch -> affOrgMatch.getMatchStrength() > 0);
        
        return affMatchResults;
        
    }


    //------------------------ SETTERS --------------------------
    
    /**
     * List of match voters that will be used to match affiliations with organization.
     * @see #computeMatches(JavaRDD) 
     */
    public void setAffOrgMatchVoters(List<AffOrgMatchVoter> affOrgMatchVoters) {
        this.affOrgMatchVoters = affOrgMatchVoters;
    }
    
}
