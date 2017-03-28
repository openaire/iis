package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSectionsSplitter;

/**
 * Abstract match voter that splits organization name and affiliation name into sections.
 * Then it votes for match only if all of organization name sections will be
 * found in affiliation name sections.
 * 
 * @author madryk
 */
public abstract class AbstractSectionedMatchVoter extends AbstractAffOrgMatchVoter {
    
    private static final long serialVersionUID = 1L;
    
    private final OrganizationSectionsSplitter sectionsSplitter = new OrganizationSectionsSplitter();
    
    
    
   
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns true if all of the organization sections in at least one of the {@link #getOrganizationNames(AffMatchOrganization)} 
     * match with affiliation sections in {@link AffMatchAffiliation}.
     */
    @Override
    public final boolean voteMatch(AffMatchAffiliation affiliation, AffMatchOrganization organization) {
        
        Preconditions.checkNotNull(affiliation);
        Preconditions.checkNotNull(organization);
        
        List<String> affSections = sectionsSplitter.splitToSections(affiliation.getOrganizationName());
        
        if (affSections.isEmpty()) {
            return false;
        }
        
        for (String orgName : getOrganizationNames(organization)) {
            
            List<String> orgSections = sectionsSplitter.splitToSections(orgName);
        
            if (orgSections.isEmpty()) {
                continue;
            }
            
            if (areAllOrgSectionsInAffSections(affSections, orgSections)) {
                return true;
            }
        }
        
        return false;
    }

       
    /**
     * Returns true if affOrgNameSections contains orgNameSection.<br/>
     * Implementations of this method can check if affOrgNameSections contains
     * orgNameSection based on equality or similarity of strings.
     */
    protected abstract boolean containsOrgSection(List<String> affOrgNameSections, String orgNameSection);
    
    /**
     * Returns the names of the passed organizations that will be used by a given implementation of this voter
     */
    protected abstract List<String> getOrganizationNames(AffMatchOrganization organization);
    
    
    //------------------------ PRIVATE --------------------------
    
    private boolean areAllOrgSectionsInAffSections(List<String> affSections, List<String> orgSections) {
        
        for (String orgSection : orgSections) {
        
            if (!containsOrgSection(affSections, orgSection)) {
                return false;
            }
        
        } 
        return true;
    }

   
    
    
}
