package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;

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
    
    private OrganizationSectionsSplitter sectionsSplitter = new OrganizationSectionsSplitter();
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns true if all of the organization sections in {@link AffMatchOrganization} 
     * matches to affiliation sections in {@link AffMatchAffiliation}.
     */
    @Override
    public final boolean voteMatch(AffMatchAffiliation affiliation, AffMatchOrganization organization) {
        
        List<String> affSections = sectionsSplitter.splitToSections(getAffiliationName(affiliation));
        List<String> orgSections = sectionsSplitter.splitToSections(getOrganizationName(organization));
        
        if (affSections.isEmpty() || orgSections.isEmpty()) {
            return false;
        }
        
        
        for (String orgSection : orgSections) {
            
            if (!containsOrgSection(affSections, orgSection)) {
                return false;
            }
            
        }
        
        return true;
    }
    
    /**
     * Returns affiliation name from {@link AffMatchAffiliation} object.
     */
    protected abstract String getAffiliationName(AffMatchAffiliation affiliation);
    
    /**
     * Returns organization name from {@link AffMatchOrganization} object.
     */
    protected abstract String getOrganizationName(AffMatchOrganization organization);
    
    /**
     * Returns true if affOrgNameSections contains orgNameSection.<br/>
     * Implementations of this method can check if affOrgNameSections contains
     * orgNameSection based on equality or similarity of strings.
     */
    protected abstract boolean containsOrgSection(List<String> affOrgNameSections, String orgNameSection);
    
    
}
