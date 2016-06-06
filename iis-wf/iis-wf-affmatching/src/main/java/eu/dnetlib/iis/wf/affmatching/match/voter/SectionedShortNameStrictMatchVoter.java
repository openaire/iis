package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * Match voter that splits {@link AffMatchAffiliation#getOrganizationName()}
 * and {@link AffMatchOrganization#getShortName()} into sections.<br/>
 * This voter votes for match if all of the organization sections
 * have equal sections in affiliation organization name.<br/>
 * 
 * @author madryk
 */
public class SectionedShortNameStrictMatchVoter extends AbstractSectionedMatchVoter {
    
    private static final long serialVersionUID = 1L;
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns {@link AffMatchAffiliation#getOrganizationName()} of the passed affiliation.
     */
    @Override
    protected String getAffiliationName(AffMatchAffiliation affiliation) {
        return affiliation.getOrganizationName();
    }
    
    /**
     * Returns {@link AffMatchOrganization#getShortName()} of the passed organization.
     */
    @Override
    protected String getOrganizationName(AffMatchOrganization organization) {
        return organization.getShortName();
    }
    
    
    /**
     * Returns true if one of the affOrgNameSections is equal to orgNameSection.
     */
    @Override
    protected boolean containsOrgSection(List<String> affOrgNameSections, String orgNameSection) {
        return affOrgNameSections.contains(orgNameSection);
    }

    
    //------------------------ toString --------------------------

    @Override
    public String toString() {
        return "SectionedShortNameStrictMatchVoter []";
    }
}
