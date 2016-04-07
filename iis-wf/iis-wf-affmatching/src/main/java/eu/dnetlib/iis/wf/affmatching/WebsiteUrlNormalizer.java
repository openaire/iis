package eu.dnetlib.iis.wf.affmatching;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.iis.common.string.StringNormalizer;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * Normalizer of an organization's website url. See {@link AffMatchOrganization#getWebsiteUrl()}
 * 
* @author Łukasz Dumiszewski
*/

public class WebsiteUrlNormalizer implements StringNormalizer {

    
    /**
     * Normalizes the given value:
     * <ul>
     * <li>converts all characters in the given value to lower cases</li>
     * <li>replaces whitespace-gaps with single-space gaps</li>
     * <li>removes http[s]:// from the beginning</li>
     * <li>removes www. from the beginning</li>
     * </ul>
     */
    @Override
    public String normalize(String value) {

        if (StringUtils.isBlank(value)) {
            return "";
        }
        
        String result = value.toLowerCase();
        
        result = result.trim().replaceAll("\\s", " ").replaceAll(" +", " ");
        
        result = result.replaceAll("^(http|https)\\:\\/\\/", "");
        result = result.replaceAll("^(www)\\.", "");
        result = result.trim();
        
        return result;
    }

}
