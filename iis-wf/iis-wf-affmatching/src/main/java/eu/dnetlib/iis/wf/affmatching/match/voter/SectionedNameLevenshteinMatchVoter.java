package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * @author madryk
 */
public class SectionedNameLevenshteinMatchVoter extends AbstractSectionedMatchVoter {
    
    private static final long serialVersionUID = 1L;
    
    
    private double minSimilarity;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    public SectionedNameLevenshteinMatchVoter(double minSimilarity) {
        this.minSimilarity = minSimilarity;
    }
    
    
    //------------------------ LOGIC --------------------------
    
    @Override
    protected String getAffiliationName(AffMatchAffiliation affiliation) {
        return affiliation.getOrganizationName();
    }
    
    
    @Override
    protected String getOrganizationName(AffMatchOrganization organization) {
        return organization.getName();
    }
    
    
    @Override
    protected boolean containsMatchingSection(List<String> sections, String sectionToFind) {
        for (String section : sections) {
            
            int distance = StringUtils.getLevenshteinDistance(section, sectionToFind);
            int maxCharacters = Math.max(section.length(), sectionToFind.length());
            
            double similarity = 1 - (double)distance/maxCharacters;
            
            if (similarity >= minSimilarity) {
                return true;
            }
        }
        
        return false;
    }


    //------------------------ toString --------------------------
    
    @Override
    public String toString() {
        return "SectionedNameLevenshteinMatchVoter [minSimilarity=" + minSimilarity + "]";
    }
}
