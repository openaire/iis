package eu.dnetlib.iis.common.string;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class SimpleStringNormalizerTest {

    private LenientComparisonStringNormalizer normalizer = new LenientComparisonStringNormalizer();
    
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void normalize_null() {
        
        // execute & assert
        assertEquals("", normalizer.normalize(null));
        
    }

    
    @Test
    public void normalize_blank() {
        
        // execute & assert
        assertEquals("", normalizer.normalize("   \t "));
        
    }

    
    @Test
    public void normalize_not_digit_or_letter_whitespaces() {
        
        // given
        String value = "Aloha II scooby doo! part\t\t XIX";
        
        // execute & assert
        assertEquals("aloha ii scooby doo part xix", normalizer.normalize(value));
    }
    
    
    @Test
    public void normalize_russian() {
        
        // given
        String value = "Квантовый размерный эффект в трехмерных микрокристаллах полупроводников";
        
        // execute & assert
        assertEquals("квантовыи размерныи эффект в трехмерных микрокристаллах полупроводников", normalizer.normalize(value));
    }

    
    @Test
    public void normalize_diacritis() {
        
        // given
        String value = "Hello Haße";
        
        // execute & assert
        assertEquals("hello hasse", normalizer.normalize(value));
    }

}
