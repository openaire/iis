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
    
    @Override
    public String normalize(String value) {
        
        String filteredValue = StringUtils.replacePattern(value, "\\(.*?\\)", "");
        
        return stringNormalizer.normalize(filteredValue);
    }

}
