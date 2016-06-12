package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.Collection;

import org.apache.commons.lang3.StringUtils;

/**
 * Class containing helper methods checking strings similarity.
 * 
 * @author madryk
 */
class StringSimilarityChecker {


    //------------------------ LOGIC --------------------------
    
    /**
     * Returns true if provided string values contains at least one
     * value similar to searchValue string.<br/>
     * Similarity is measured based on Jaro-Winkler distance.<br/>
     * Strings must have similarity greater or equal to minSimilarity
     * to be found similar.
     * 
     * @see StringUtils#getJaroWinklerDistance(CharSequence, CharSequence)
     */
    public boolean containSimilarString(Collection<String> values, String searchValue, double minSimilarity) {
        
        for (String value : values) {
            double similarity = StringUtils.getJaroWinklerDistance(value, searchValue);
            
            if (similarity >= minSimilarity) {
                return true;
            }
        }
        return false;
    }
}
