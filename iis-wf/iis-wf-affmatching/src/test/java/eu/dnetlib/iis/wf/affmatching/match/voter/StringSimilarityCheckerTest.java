package eu.dnetlib.iis.wf.affmatching.match.voter;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author madryk
 */
public class StringSimilarityCheckerTest {

    private StringSimilarityChecker similarityChecker = new StringSimilarityChecker();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void containsSimilarString() {
        
        // execute & assert
        assertTrue(similarityChecker.containsSimilarString(ImmutableSet.of("danmarks", "tekniske", "universitet"), "university", 0.8));
        
        // Jaro-Winkler similarity: [university] [universitet] 0.94
    }
    
    @Test
    public void containsSimilarString_SIMILAR_NOT_FOUND() {
        
        // execute & assert
        assertFalse(similarityChecker.containsSimilarString(ImmutableSet.of("danmarks", "tekniske", "universitet"), "technical", 0.8));
        
        // Jaro-Winkler similarity: [technical] [danmarks] 0.49
        // Jaro-Winkler similarity: [technical] [tekniske] 0.72
        // Jaro-Winkler similarity: [technical] [universitet] 0.42
    }
}
