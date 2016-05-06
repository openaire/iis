package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
* @author Łukasz Dumiszewski
*/

public class NameStrictWithCharFilteringMatchVoter implements AffOrgMatchVoter {

    
    private static final long serialVersionUID = 1L;

    private List<Character> charsToFilter;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    /**
     * @param charsToFilter - list of characters that will be not taken
     *      into account when comparing organization names
     */
    public NameStrictWithCharFilteringMatchVoter(List<Character> charsToFilter) {
        this.charsToFilter = charsToFilter;
    }
    
    //------------------------ LOGIC --------------------------
    
    
    /**
     * Returns true if the name of the passed organization is the same as the name of the organization in the passed affiliation
     * (does not take chars to filter into account). 
     */
    @Override
    public boolean voteMatch(AffMatchAffiliation affiliation, AffMatchOrganization organization) {
        
        String filteredAffName = filterName(affiliation.getOrganizationName());
        String filteredOrgName = filterName(organization.getName());
        
        
        if (StringUtils.isEmpty(filteredAffName) || StringUtils.isEmpty(filteredOrgName)) {
            return false;
        }
        
        return filteredAffName.equals(filteredOrgName);
    }
    
    private String filterName(String name) {
        
        String filteredName = name;
        
        for (Character charToFilter : charsToFilter) {
            filteredName = StringUtils.remove(filteredName, charToFilter);
        }
        
        return filteredName;
    }
}
