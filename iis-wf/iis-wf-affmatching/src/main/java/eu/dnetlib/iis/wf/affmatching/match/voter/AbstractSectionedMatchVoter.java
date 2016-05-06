package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * Abstract match voter that splits organization name and affiliation name into sections.
 * Then it votes for match only if all of organization name sections will be
 * found in affiliation name sections.
 * 
 * @author madryk
 */
public abstract class AbstractSectionedMatchVoter implements AffOrgMatchVoter {
    
    private static final long serialVersionUID = 1L;
    
    
    //------------------------ LOGIC --------------------------
    
    @Override
    public boolean voteMatch(AffMatchAffiliation affiliation, AffMatchOrganization organization) {
        
        List<String> affSections = OrganizationSectionsSplitter.splitToSections(getAffiliationName(affiliation));
        List<String> orgSections = OrganizationSectionsSplitter.splitToSections(getOrganizationName(organization));
        
        if (affSections.isEmpty() || orgSections.isEmpty()) {
            return false;
        }
        
        
        for (String orgSection : orgSections) {
            
            if (!containsMatchingSection(affSections, orgSection)) {
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
     * Returns true if sectionToFind matches to one of the section in provided list.
     */
    protected abstract boolean containsMatchingSection(List<String> sections, String sectionToFind);
    
    
}
