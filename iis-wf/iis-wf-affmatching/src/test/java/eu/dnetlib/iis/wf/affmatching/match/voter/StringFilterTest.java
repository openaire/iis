package eu.dnetlib.iis.wf.affmatching.match.voter;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author madryk
 */
public class StringFilterTest {

    private StringFilter stringFilter = new StringFilter();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void filterChars() {
        
        // execute & assert
        assertEquals("ome word more word", stringFilter.filterChars("some words, more words", ImmutableList.of(',', 's')));
    }
    
    @Test
    public void filterShorWords() {
        
        // execute & assert
        assertEquals("cde", stringFilter.filterShortWords("a bc cde fg h", 2));
    }
    
    @Test
    public void filterShortWords_ZERO_LENGTH() {
        
        // execute & assert
        assertEquals("a bc cde fg h", stringFilter.filterShortWords("a bc cde fg h", 0));
    }
    
    @Test
    public void filterCharsAndShortWords() {
        
        // execute & assert
        assertEquals("abcd cde", stringFilter.filterCharsAndShortWords("abcd, bc cde, fg h", ImmutableList.of(','), 2));
    }
}
