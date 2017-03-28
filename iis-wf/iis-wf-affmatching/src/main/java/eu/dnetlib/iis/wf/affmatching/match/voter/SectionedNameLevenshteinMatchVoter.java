package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Objects;
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
    
    
    private final double minSimilarity;
    
    private Function<AffMatchOrganization, List<String>> getOrgNamesFunction = new GetOrgNameFunction();
    
    
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
        super();
        Preconditions.checkArgument(minSimilarity > 0 && minSimilarity <= 1);
        this.minSimilarity = minSimilarity;
    }
    
    
    //------------------------ LOGIC --------------------------
    
    
    
    /**
     * Returns true if any of the affiliation name sections is similar to 
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
                .add("minSimilarity", minSimilarity)
                .add("getOrgNamesFunction", getOrgNamesFunction.getClass().getSimpleName())
                .toString();
    }
    
}
