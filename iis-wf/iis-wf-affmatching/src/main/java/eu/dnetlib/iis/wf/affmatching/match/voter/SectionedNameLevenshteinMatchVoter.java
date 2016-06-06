package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * Match voter that splits {@link AffMatchAffiliation#getOrganizationName()}
 * and {@link AffMatchOrganization#getName()} into sections.<br/>
 * This voter votes for match if all of the organization sections
 * have similar sections in affiliation organization name.<br/>
 * Similarity is measured based on Levenshtein distance.
 * 
 * @author madryk
 */
public class SectionedNameLevenshteinMatchVoter extends AbstractSectionedMatchVoter {
    
    private static final long serialVersionUID = 1L;
    
    
    private double minSimilarity;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    /**
     * Default constructor
     * 
     * @param minSimilarity - minimum similarity for two sections to be found similar.
     *      Value must be between (0,1] (minimum similarity equal to one means
     *      that two sections must be equal).
     * @see #containsOrgSection(List, String)
     */
    public SectionedNameLevenshteinMatchVoter(double minSimilarity) {
        Preconditions.checkArgument(minSimilarity > 0 && minSimilarity <= 1);
        this.minSimilarity = minSimilarity;
    }
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns {@link AffMatchAffiliation#getOrganizationName()} of the passed affiliation.
     */
    @Override
    protected String getAffiliationName(AffMatchAffiliation affiliation) {
        return affiliation.getOrganizationName();
    }
    
    /**
     * Returns {@link AffMatchOrganization#getName()} of the passed organization.
     */
    @Override
    protected String getOrganizationName(AffMatchOrganization organization) {
        return organization.getName();
    }
    
    /**
     * Returns true if one of the affiliation name section is similar to 
     * the organization name section.<br/>
     * Similarity is measured based on Levenshtein distance according to
     * the following formula:<br/>
     * <code>similarity = 1 - (levenshteinDistance(a, b) / max(a.length(), b.length()))</code><br/>
     * where <code>a</code> is affiliation name section and <code>b</code>
     * is organization name section.
     */
    @Override
    protected boolean containsOrgSection(List<String> affOrgNameSections, String orgNameSection) {
        for (String section : affOrgNameSections) {
            
            int distance = StringUtils.getLevenshteinDistance(section, orgNameSection);
            int maxCharacters = Math.max(section.length(), orgNameSection.length());
            
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
