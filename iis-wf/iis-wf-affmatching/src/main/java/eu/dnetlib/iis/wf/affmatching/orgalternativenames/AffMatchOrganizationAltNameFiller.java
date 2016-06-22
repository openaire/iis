package eu.dnetlib.iis.wf.affmatching.orgalternativenames;

import static java.util.stream.Collectors.toList;

import java.io.Serializable;
import java.util.List;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * Filler of {@link AffMatchOrganization#getAlternativeNames()} list
 * based on a dictionary of alternative organization names.
 * 
 * @author madryk
 */
public class AffMatchOrganizationAltNameFiller implements Serializable {

    private static final long serialVersionUID = 1L;
    
    
    private List<List<String>> alternativeNamesDictionary = Lists.newArrayList();
    

    //------------------------ LOGIC --------------------------
    
    /**
     * Fills {@link AffMatchOrganization#getAlternativeNames()} list with
     * alternative names found in dictionary.<br/>
     * Method is looking for alternative names in dictionary
     * based on equality of {@link AffMatchOrganization#getName()}
     * or any of the {@link AffMatchOrganization#getAlternativeNames()} that
     * was already present in passed organization.
     * 
     * Notice that method modifies {@link AffMatchOrganization#getAlternativeNames()}
     * of the passed organization.<br/>
     */
    public AffMatchOrganization fillAlternativeNames(AffMatchOrganization organization) {
        
        List<String> alternativeNames = fetchAlternativeNames(organization.getNames());
        
        alternativeNames.stream().forEach(altName -> organization.addAlternativeName(altName));
        
        return organization;
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private List<String> fetchAlternativeNames(List<String> originalNames) {
        
        for (List<String> alternativeNames : alternativeNamesDictionary) {
            
            if (containsSameOrganizationName(originalNames, alternativeNames)) {
                return filterAlreadyPresentAlternativeNames(alternativeNames, originalNames);
            }
            
        }
        return Lists.newArrayList();
    }
    
    private boolean containsSameOrganizationName(List<String> originalNames, List<String> alternativeNames) {
        
        return originalNames.stream().anyMatch(orgName -> alternativeNames.contains(orgName));
        
    }
    
    private List<String> filterAlreadyPresentAlternativeNames(List<String> alternativeNames, List<String> originalNames) {
        
        return alternativeNames.stream().filter(altName -> !originalNames.contains(altName)).collect(toList());
    }
    
    
    //------------------------ SETTERS --------------------------
    
    /**
     * Dictionary of alternative organization names
     */
    public void setAlternativeNamesDictionary(List<List<String>> alternativeNamesDictionary) {
        this.alternativeNamesDictionary = alternativeNamesDictionary;
    }
    
    
}
