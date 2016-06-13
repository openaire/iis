package eu.dnetlib.iis.wf.affmatching.match.voter;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * Voter that checks equality of country codes in affiliation
 * and organization.
 * It checks country codes stricty (if one or both of them are empty
 * then voter will assume that they are not equal)
 * 
 * @author madryk
 */
public class CountryCodeStrictMatchVoter extends AbstractAffOrgMatchVoter {

    private static final long serialVersionUID = 1L;
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns true if {@link AffMatchAffiliation#getCountryCode()} and
     * {@link AffMatchOrganization#getCountryCode()} are non-empty and equal.
     */
    @Override
    public boolean voteMatch(AffMatchAffiliation affiliation, AffMatchOrganization organization) {
        
        if (StringUtils.isEmpty(affiliation.getCountryCode()) || StringUtils.isEmpty(organization.getCountryCode())) {
            return false;
        }
        
        return affiliation.getCountryCode().equals(organization.getCountryCode());
    }


}
