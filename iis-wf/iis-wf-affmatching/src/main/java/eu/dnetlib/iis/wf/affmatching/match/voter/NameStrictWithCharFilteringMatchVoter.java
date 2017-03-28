package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Objects;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
* @author Łukasz Dumiszewski
*/

public class NameStrictWithCharFilteringMatchVoter extends AbstractAffOrgMatchVoter {

    
    private static final long serialVersionUID = 1L;

    private final StringFilter stringFilter = new StringFilter();
    
    private final List<Character> charsToFilter;
    
    private Function<AffMatchOrganization, List<String>> getOrgNamesFunction = new GetOrgNameFunction();
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    /**
     * @param charsToFilter - list of characters that will be not taken
     *      into account when comparing organization names
     */
    public NameStrictWithCharFilteringMatchVoter(List<Character> charsToFilter) {
        super();
        this.charsToFilter = charsToFilter;
    }
    
    //------------------------ LOGIC --------------------------
    
    
    /**
     * Returns true if at least one of the names returned by the getOrgNamesFunction  
     * (see {@link #setGetOrgNamesFunction(Function)} is the same as the name of the organization in the passed affiliation
     * (before comparison, the names are stripped of the 'chars to filter' passed to the constructor). 
     */
    @Override
    public boolean voteMatch(AffMatchAffiliation affiliation, AffMatchOrganization organization) {
        
        String filteredAffName = stringFilter.filterChars(affiliation.getOrganizationName(), charsToFilter);
        
        
        if (StringUtils.isEmpty(filteredAffName)) {
            return false;
        }
        
        for (String orgName : getOrgNamesFunction.apply(organization)) {
            
            String filteredOrgName = stringFilter.filterChars(orgName, charsToFilter);
            if (StringUtils.isEmpty(filteredOrgName)) {
                continue;    
            }
            
            if (filteredAffName.equals(filteredOrgName)) {
                return true;
            }
        }
        
        
        return false;
    }

    
    //------------------------ SETTERS --------------------------
    
    /**
     * Sets the function that will be used to get the organization names 
     */
    public void setGetOrgNamesFunction(Function<AffMatchOrganization, List<String>> getOrgNamesFunction) {
        this.getOrgNamesFunction = getOrgNamesFunction;
    }
   
    
    //------------------------ toString --------------------------
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("matchStrength", getMatchStrength())
                .add("getOrgNamesFunction", getOrgNamesFunction.getClass().getSimpleName())
                .add("charsToFilter", charsToFilter)
                .toString();
    }

   
}
