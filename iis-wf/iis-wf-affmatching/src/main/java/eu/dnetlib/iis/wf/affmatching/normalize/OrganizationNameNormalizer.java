package eu.dnetlib.iis.wf.affmatching.normalize;

import java.io.Serializable;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import eu.dnetlib.iis.common.string.LenientComparisonStringNormalizer;
import eu.dnetlib.iis.common.string.StringNormalizer;
import eu.dnetlib.iis.wf.affmatching.utils.PatternUtils;

/**
 * {@link StringNormalizer} that normalizes organization name string
 * 
 * @author madryk
 */
public class OrganizationNameNormalizer implements StringNormalizer, Serializable {

    private static final long serialVersionUID = 1L;
    
    
    private StringNormalizer innerNormalizer = new LenientComparisonStringNormalizer(ImmutableList.of(',', ';'));
    
    private Set<String> stopwords = Sets.newHashSet("of");
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Normalizes the given organization name:<br/>
     * <ul>
     * <li>removes any text between brackets</li>
     * <li>performs inner normalization (using {@link LenientComparisonStringNormalizer} by default)</li>
     * <li>removes stopwords (defined in {@link #setStopwords(Set)})</li>
     * </ul>
     * First it removes any text between brackets and then
     * it uses internal {@link StringNormalizer} for further normalization.
     */
    @Override
    public String normalize(String organizationName) {
        
        if (StringUtils.isBlank(organizationName)) {
            return "";
        }
        
        // remove brackets
        String filteredOrganizationName = PatternUtils.removePattern(organizationName, "\\(.*?\\)");
        
        // internal normalization
        filteredOrganizationName = innerNormalizer.normalize(filteredOrganizationName);
        
        // remove stopwords
        for (String stopword : stopwords) {
            filteredOrganizationName = PatternUtils.removePattern(filteredOrganizationName, "\\b" + stopword + "\\b");
        }
        filteredOrganizationName = filteredOrganizationName.trim().replaceAll(" +", " ");
        
        return filteredOrganizationName;
    }
    
    
    //------------------------ SETTERS --------------------------
    
    public void setInnerNormalizer(StringNormalizer innerNormalizer) {
        this.innerNormalizer = innerNormalizer;
    }

    public void setStopwords(Set<String> stopwords) {
        this.stopwords = stopwords;
    }

}
