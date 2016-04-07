package eu.dnetlib.iis.common.string;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
* @author Łukasz Dumiszewski
*/

public class DiacriticsRemoverTest {

   
    //------------------------ TESTS --------------------------
    
    @Test
    public void removeDiacritics() {
        
        // given
        String value = "\u0301aáLEßæ Łók";
        
        // execute & assert
        assertEquals("aaLEssae Lok", DiacriticsRemover.removeDiacritics(value));
        
    }
    
}
