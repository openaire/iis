package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * Match voter that may contain multiple match voters.
 * 
 * @author madryk
 *
 */
public class CompositeMatchVoter extends AbstractAffOrgMatchVoter {

    private static final long serialVersionUID = 1L;
    
    private List<AffOrgMatchVoter> voters;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    public CompositeMatchVoter(List<AffOrgMatchVoter> voters) {
        Preconditions.checkNotNull(voters);
        Preconditions.checkArgument(voters.size() > 0);
        
        this.voters = voters;
    }
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns true only if all of underlying match voters
     * return true for given affiliation and organization. 
     */
    @Override
    public boolean voteMatch(AffMatchAffiliation affiliation, AffMatchOrganization organization) {
        
        for (AffOrgMatchVoter matchVoter : voters) {
            
            if (!matchVoter.voteMatch(affiliation, organization)) {
                return false;
            }
            
        }
        
        return true;
    }

    
    //------------------------ toString --------------------------

    @Override
    public String toString() {
        return "CompositeMatchVoter [voters=" + voters + "]";
    }

}
