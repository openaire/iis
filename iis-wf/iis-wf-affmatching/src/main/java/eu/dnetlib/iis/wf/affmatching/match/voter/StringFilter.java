package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/**
 * Class containing helper methods for filtering strings.
 * 
 * @author madryk
 */
class StringFilter implements Serializable {

    private static final long serialVersionUID = 1L;
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns filtered value without charsToFilter and without
     * words shorter than wordToRemoveMaxLength.<br/>
     * Internally uses {@link #filterChars(String, List)} and 
     * {@link #filterShortWords(String, int)}.
     */
    public String filterCharsAndShortWords(String value, List<Character> charsToFilter, int wordToRemoveMaxLength) {
        String filteredValue = value;
        
        filteredValue = filterChars(filteredValue, charsToFilter);
        filteredValue = filterShortWords(filteredValue, wordToRemoveMaxLength);
        
        return filteredValue;
    }
    
    /**
     * Returns filtered value without charsToFilter.
     */
    public String filterChars(String value, List<Character> charsToFilter) {
        
        String filteredValue = value;
        
        for (Character charToFilter : charsToFilter) {
            filteredValue = StringUtils.remove(filteredValue, charToFilter);
        }
        
        return filteredValue;
    }
    
    /**
     * Returns filtered value without words shorter than wordToRemoveMaxLength.<br/>
     * When wordToRemoveMaxLength is zero then returns the value unchanged.
     */
    public String filterShortWords(String value, int wordToRemoveMaxLength) {
        
        if (wordToRemoveMaxLength == 0) {
            return value;
        }
        
        String filteredValue = value;
        
        filteredValue = StringUtils.removePattern(filteredValue, "\\b\\w{1," + wordToRemoveMaxLength + "}\\b");
        filteredValue = filteredValue.trim().replaceAll(" +", " ");
        
        return filteredValue;
    }
}
