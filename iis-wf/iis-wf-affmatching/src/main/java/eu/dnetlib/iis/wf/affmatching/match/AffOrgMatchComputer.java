package eu.dnetlib.iis.wf.affmatching.match;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.NameCountryStrictMatchVoter;
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

    
    private List<AffOrgMatchVoter> affOrgMatchVoters = new ArrayList<>();
    
    private AffOrgMatchVoterStrengthCalculator voterStrengthCalculator = new AffOrgMatchVoterStrengthCalculator();

    private AffOrgMatchStrengthRecalculator affOrgMatchStrengthRecalculator = new AffOrgMatchStrengthRecalculator();
    
    
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    public AffOrgMatchComputer() {
        
        affOrgMatchVoters.add(new NameCountryStrictMatchVoter());
        
    }
    
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Computes the match strength of every passed pair of affiliation and organization. The returned strengths are in the range
     * of (0, 1>. The pairs which have the match strength equal to 0 are filtered out and not returned.<br/><br/>
     * The match strength of the given affiliation-organization pair depends on how many of the {@link #setAffOrgMatchVoters(List)}
     * say the pair matches.
     */
    public JavaRDD<AffMatchResult> computeMatches(JavaRDD<Tuple2<AffMatchAffiliation, AffMatchOrganization>> joinedAffOrgs) {
        
        Preconditions.checkNotNull(joinedAffOrgs);
        
        
        JavaRDD<AffMatchResult> affMatchResults = joinedAffOrgs.map(kv->new AffMatchResult(kv._1(), kv._2(), 0));
        
        
        int voterStrengthSum = 0;
        
        for (int i = 0; i < affOrgMatchVoters.size(); i++) {
            
            int voterStrength = voterStrengthCalculator.calculateStrength(i, affOrgMatchVoters.size());
            
            voterStrengthSum += voterStrength;
            
            AffOrgMatchVoter eqVoter = affOrgMatchVoters.get(i);
            
            affMatchResults = affMatchResults.map(affOrgMatch ->  affOrgMatchStrengthRecalculator.recalculateMatchStrength(affOrgMatch, eqVoter, voterStrength));
            
        }
        
        affMatchResults = affMatchResults.filter(affOrgMatch -> affOrgMatch.getMatchStrength() > 0);
        
        
        int maxVoterStrength = voterStrengthSum;

        return affMatchResults.map(affOrgMatch -> new AffMatchResult(affOrgMatch.getAffiliation(), affOrgMatch.getOrganization(), affOrgMatch.getMatchStrength()/maxVoterStrength));
        
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
