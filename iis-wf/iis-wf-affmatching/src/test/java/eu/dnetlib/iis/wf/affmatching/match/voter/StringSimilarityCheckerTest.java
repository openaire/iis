package eu.dnetlib.iis.wf.affmatching.match.voter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;

/**
 * @author madryk
 */
public class StringSimilarityCheckerTest {

    private StringSimilarityChecker similarityChecker = new StringSimilarityChecker();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void containsSimilarString() {
        
        // execute & assert
        assertTrue(similarityChecker.containSimilarString(ImmutableSet.of("danmarks", "tekniske", "universitet"), "university", 0.8));
        
        // Jaro-Winkler similarity: [university] [universitet] 0.94
    }
    
    @Test
    public void containsSimilarString_SIMILAR_NOT_FOUND() {
        
        // execute & assert
        assertFalse(similarityChecker.containSimilarString(ImmutableSet.of("danmarks", "tekniske", "universitet"), "technical", 0.8));
        
        // Jaro-Winkler similarity: [technical] [danmarks] 0.49
        // Jaro-Winkler similarity: [technical] [tekniske] 0.72
        // Jaro-Winkler similarity: [technical] [universitet] 0.42
    }
}
