package eu.dnetlib.iis.wf.affmatching.normalize;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author madryk
 */
public class CountryCodeNormalizerTest {

    private CountryCodeNormalizer normalizer = new CountryCodeNormalizer();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void normalize_NULL() {
        // execute & assert
        assertEquals("", normalizer.normalize(null));
    }
    
    @Test
    public void normalize_BLANK() {
        // execute & assert
        assertEquals("", normalizer.normalize("  "));
    }
    
    @Test
    public void normalize_UPPERCASED() {
        // execute & assert
        assertEquals("pl", normalizer.normalize(" PL"));
    }
    
    @Test
    public void normalize_LOWERCASED() {
        // execute & assert
        assertEquals("pl", normalizer.normalize("pl "));
    }
    
    @Test
    public void normalize_INCORRECT() {
        // execute & assert
        assertEquals("gb", normalizer.normalize(" UK"));
    }
}
