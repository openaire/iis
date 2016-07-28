package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.io.Serializable;
import java.util.Collection;

/**
 * Class containing helper methods checking strings similarity.
 * 
 * @author madryk
 */
class StringSimilarityChecker implements Serializable {

    private static final long serialVersionUID = 1L;


    //------------------------ LOGIC --------------------------
    
    /**
     * Returns true if provided string values contain at least one
     * value similar to searchValue string.<br/>
     * The similarity is measured based on Jaro-Winkler distance.<br/>
     * Strings must have similarity greater or equal to minSimilarity
     * to be found similar.
     * 
     * @see JaroWinklerDistanceCalculator#getDistance(String, String)
     */
    public boolean containSimilarString(Collection<String> values, String searchValue, double minSimilarity) {
        
        for (String value : values) {
            double similarity = JaroWinklerDistanceCalculator.getDistance(value, searchValue); 
                                    // change it to {@link StringUtils#getJaroWinklerDistance(String, String)
                                    // after the common-lang3 3.5 is released
                                    // version 3.4 is not correct, see: https://issues.apache.org/jira/browse/LANG-1199
            
            if (similarity >= minSimilarity) {
                return true;
            }
        }
        return false;
    }
}
