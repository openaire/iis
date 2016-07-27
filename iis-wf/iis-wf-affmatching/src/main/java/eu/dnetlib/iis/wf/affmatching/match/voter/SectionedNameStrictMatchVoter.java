package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;
import java.util.function.Function;

import com.google.common.base.Objects;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * Match voter that splits each of the organization names into sections.<br/>
 * The affiliation and organization matches if all of the organization sections (of at lease one of the organization names)
 * have equal sections in affiliation organization name.<br/>
 * 
 * @author madryk
 */
public class SectionedNameStrictMatchVoter extends AbstractSectionedMatchVoter {
    
    private static final long serialVersionUID = 1L;
    
    private Function<AffMatchOrganization, List<String>> getOrgNamesFunction = new GetOrgNameFunction();
    
    
    
        
    //------------------------ LOGIC --------------------------
     
    
    
    /**
     * Returns true if any of the affOrgNameSections is equal to orgNameSection.
     */
    @Override
    protected boolean containsOrgSection(List<String> affOrgNameSections, String orgNameSection) {
        return affOrgNameSections.contains(orgNameSection);
    }

    
    @Override
    protected List<String> getOrganizationNames(AffMatchOrganization organization) {
        return getOrgNamesFunction.apply(organization);
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
                .add("getOrgNamesFunction", getOrgNamesFunction.getClass().getSimpleName())
                .toString();
    }

   
  
    
}
