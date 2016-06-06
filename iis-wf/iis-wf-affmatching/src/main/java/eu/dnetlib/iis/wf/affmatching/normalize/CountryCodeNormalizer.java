package eu.dnetlib.iis.wf.affmatching.normalize;

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableMap;

import eu.dnetlib.iis.common.string.StringNormalizer;

/**
 * Normalizer of country codes.
 * 
 * @author madryk
 */
public class CountryCodeNormalizer implements StringNormalizer, Serializable {

    private static final long serialVersionUID = 1L;
    
    
    private static final Map<String, String> COUNTRY_CODE_REPLACEMENTS = ImmutableMap.of("uk", "gb");
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Normalizes the given country code:
     * <ul>
     * <li>converts the given value to lowercase</li>
     * <li>trims it</li>
     * <li>replaces common incorrect country code values to the correct ones 
     * (e.g. <code>uk</code> to <code>gb</code></li>
     * </ul>
     */
    @Override
    public String normalize(String countryCode) {
        
        if (StringUtils.isBlank(countryCode)) {
            return "";
        }
        
        String normalizedCountryCode = countryCode.toLowerCase().trim();
        
        if (COUNTRY_CODE_REPLACEMENTS.containsKey(normalizedCountryCode)) {
            return COUNTRY_CODE_REPLACEMENTS.get(normalizedCountryCode);
        }
        
        return normalizedCountryCode;
    }

    
}
