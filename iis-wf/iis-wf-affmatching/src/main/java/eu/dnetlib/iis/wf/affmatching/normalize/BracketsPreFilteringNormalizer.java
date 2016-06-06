package eu.dnetlib.iis.wf.affmatching.normalize;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.iis.common.string.StringNormalizer;

/**
 * {@link StringNormalizer} that filters everything inside bracket pairs
 * before normalizing. After filtering it uses internal {@link StringNormalizer}
 * to normalize a string.
 * 
 * @author madryk
 */
public class BracketsPreFilteringNormalizer implements StringNormalizer, Serializable {

    private static final long serialVersionUID = 1L;
    
    
    private StringNormalizer stringNormalizer;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    /**
     * @param stringNormalizer - normalizer to use after brackets prefiltering
     */
    public BracketsPreFilteringNormalizer(StringNormalizer stringNormalizer) {
        this.stringNormalizer = stringNormalizer;
    }
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Normalizes the given value.<br/>
     * First it removes any text between brackets and then
     * it uses internal {@link StringNormalizer} for further normalization.
     */
    @Override
    public String normalize(String value) {
        
        String filteredValue = StringUtils.removePattern(value, "\\(.*?\\)");
        
        return stringNormalizer.normalize(filteredValue);
    }

}
