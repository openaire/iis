package eu.dnetlib.iis.common.string;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
