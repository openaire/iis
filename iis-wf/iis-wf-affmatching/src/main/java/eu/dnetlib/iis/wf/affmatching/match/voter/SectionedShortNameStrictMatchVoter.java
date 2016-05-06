package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * @author madryk
 */
public class SectionedShortNameStrictMatchVoter extends AbstractSectionedMatchVoter {
    
    private static final long serialVersionUID = 1L;
    
    
    //------------------------ LOGIC --------------------------
    
    @Override
    protected String getAffiliationName(AffMatchAffiliation affiliation) {
        return affiliation.getOrganizationName();
    }
    
    
    @Override
    protected String getOrganizationName(AffMatchOrganization organization) {
        return organization.getShortName();
    }
    
    
    @Override
    protected boolean containsMatchingSection(List<String> sections, String sectionToFind) {
        return sections.contains(sectionToFind);
    }
}
