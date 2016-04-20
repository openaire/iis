package eu.dnetlib.iis.wf.affmatching.bucket;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
* @author Łukasz Dumiszewski
*/

public class StringPartFirstLettersHasherTest {

    private StringPartFirstLettersHasher hasher = new StringPartFirstLettersHasher();
    
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = IllegalArgumentException.class)
    public void hash_numberOfParts_less_than_1() {
        
        // execute & assert
        
        hasher.hash("Alice has a cat", 0, 2);
        
    }

    
    @Test(expected = IllegalArgumentException.class)
    public void hash_numberOfLettersPerPart_less_than_1() {
        
        // execute & assert
        
        hasher.hash("Alice has a cat", 1, 0);
        
    }
    
    
    @Test
    public void hash_value_blank() {
        
        // execute & assert
        
        assertEquals("", hasher.hash(" ", 1, 1));
        
    }
    
    
    @Test
    public void hash_value_null() {
        
        // execute & assert
        
        assertEquals("", hasher.hash(" ", 1, 1));
        
    }
    
    
    @Test
    public void hash_long_words() {
        
        // execute & assert
        
        assertEquals("Alha", hasher.hash("Alice has a cat", 2, 2));
        
    }

    
    @Test
    public void hash_short_words() {
        
        // execute & assert
        
        assertEquals("Al__has_a___", hasher.hash("Al has a cat", 3, 4));
        
    }
    
    
    @Test
    public void hash_too_less_parts() {
        
        // execute & assert
        
        assertEquals("Al__has_", hasher.hash("Al has", 3, 4));
        
    }
    
    
}
