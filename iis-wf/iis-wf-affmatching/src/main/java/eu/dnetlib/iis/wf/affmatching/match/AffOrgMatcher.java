package eu.dnetlib.iis.wf.affmatching.match;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentOrganization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import scala.Tuple2;

/**
 * Service that actually matches {@link AffMatchAffiliation}s with {@link AffMatchOrganization}s. 
 * 
 * 
 * @author Łukasz Dumiszewski
*/

public class AffOrgMatcher implements Serializable {

    
    private static final long serialVersionUID = 1L;

    private AffOrgJoiner affOrgJoiner;
    
    private AffOrgMatchComputer affOrgMatchComputer;
    
    private BestAffMatchResultPicker bestAffMatchResultPicker = new BestAffMatchResultPicker();
    
    
    
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Matches the passed affiliation with the passed organizations. The matching algorithm consists of 3 steps:
     * <ul>
     * <li>joining the affiliations and organizations (in pairs) according to some rule, performed by {@link #setAffOrgJoiner(AffOrgJoiner)}</li>
     * <li>computing a match strength of each pair, performed by {@link #setAffOrgMatchComputer(AffOrgMatchComputer)}
     * <li>choosing the best matching affiliation-organization pair for each affiliation, done by {@link BestAffMatchResultPicker}
     * </ul> 
     */
    public JavaRDD<AffMatchResult> match(JavaRDD<AffMatchAffiliation> affiliations, JavaRDD<AffMatchOrganization> organizations, 
            JavaRDD<AffMatchDocumentOrganization> documentOrganizations) {
        
        checkNotNull(affiliations);
        checkNotNull(organizations);
        checkNotNull(documentOrganizations);
        
        checkNotNull(affOrgJoiner, "affOrgJoiner has not been set");
        checkNotNull(affOrgMatchComputer, "affOrgMatchComputer has not been set");
        
        
        JavaRDD<Tuple2<AffMatchAffiliation, AffMatchOrganization>> joinedAffOrgs = affOrgJoiner.join(affiliations, organizations, documentOrganizations);
        
        JavaRDD<AffMatchResult> matchedAffOrgs = affOrgMatchComputer.computeMatches(joinedAffOrgs);
           
        matchedAffOrgs = bestAffMatchResultPicker.pickBestAffMatchResults(matchedAffOrgs);
        
        return matchedAffOrgs;
    }

    
    
    
    //------------------------ SETTERS --------------------------
    
    
    public void setAffOrgJoiner(AffOrgJoiner affOrgJoiner) {
        this.affOrgJoiner = affOrgJoiner;
    }

    public void setAffOrgMatchComputer(AffOrgMatchComputer affOrgMatchComputer) {
        this.affOrgMatchComputer = affOrgMatchComputer;
    }

 
    
    
    
}
