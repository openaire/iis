package eu.dnetlib.iis.wf.affmatching.bucket;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
* @author Łukasz Dumiszewski
*/

public class StringPartFirstLettersHasherTest {

    private StringPartFirstLettersHasher hasher = new StringPartFirstLettersHasher();
    
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void hash_numberOfParts_less_than_1() {
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> hasher.setNumberOfParts(0));
        
    }

    
    @Test
    public void hash_numberOfLettersPerPart_less_than_1() {
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> hasher.setNumberOfLettersPerPart(0));
        
        
    }
    
    
    @Test
    public void hash_value_blank() {
        
        // execute & assert
        
        assertEquals("", hasher.hash(" "));
        
    }
    
    
    @Test
    public void hash_value_null() {
        
        // execute & assert
        
        assertEquals("", hasher.hash(null));
        
    }
    
    
    @Test
    public void hash_long_words() {
        
        // execute & assert
        
        assertEquals("Alha", hasher.hash("Alice has a cat"));
        
    }

    
    @Test
    public void hash_short_words() {
        
        // given
        
        hasher.setNumberOfParts(3);
        hasher.setNumberOfLettersPerPart(4);
        
        // execute & assert
        
        assertEquals("Al__has_a___", hasher.hash("Al has a cat"));
        
    }
    
    
    @Test
    public void hash_less_parts() {
        
        // given
        
        hasher.setNumberOfParts(3);
        hasher.setNumberOfLettersPerPart(4);
        
        
        // execute & assert
        
        assertEquals("Al__has_", hasher.hash("Al has"));
        
    }
    
    
    @Test
    public void hash_less_parts_twice() {
        
        // given
        
        hasher.setNumberOfParts(3);
        hasher.setNumberOfLettersPerPart(4);
        
        
        // execute & assert
        
        assertEquals("Al__has_", hasher.hash("Al has"));
        assertEquals("Al__has_a___", hasher.hash("Al has a cat"));
        
    }
    
    
}
