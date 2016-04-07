package eu.dnetlib.iis.wf.affmatching;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
* @author Łukasz Dumiszewski
*/

public class WebsiteUrlNormalizerTest {

    private WebsiteUrlNormalizer normalizer = new WebsiteUrlNormalizer();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void normalize_null() {
        
        // execute & assert
        
        assertEquals("", normalizer.normalize(null));
        
    }

    
    @Test
    public void normalize_blank() {
        
        // execute & assert
        
        assertEquals("", normalizer.normalize("   "));
        
    }
    
    @Test
    public void normalize_no_www() {
        
        // execute & assert
        
        assertEquals("icm.edu.pl", normalizer.normalize("icm.edu.pl"));
        
    }


    @Test
    public void normalize_no_www_space() {
        
        // execute & assert
        
        assertEquals("icm.edu.pl", normalizer.normalize("icm.edu.pl \t"));
        
    }

    
    @Test
    public void normalize_www() {
        
        // execute & assert
        
        assertEquals("icm.edu.pl", normalizer.normalize("www.icm.edu.pl"));
        
    }
    
    @Test
    public void normalize_www_www() {
        
        // execute & assert
        
        assertEquals("www.icm.edu.pl", normalizer.normalize("www.www.icm.edu.pl"));
        
    }

    @Test
    public void normalize_http_www() {
        
        // execute & assert
        
        assertEquals("icm.edu.pl", normalizer.normalize("http://www.icm.edu.pl"));
        
    }

    
    @Test
    public void normalize_https_www() {
        
        // execute & assert
        
        assertEquals("icm.edu.pl", normalizer.normalize("https://www.icm.edu.pl"));
        
    }

    
    @Test
    public void normalize_https_www_params() {
        
        // execute & assert
        
        assertEquals("icm.edu.pl/aa/ssss?ssss=2232", normalizer.normalize("https://www.icm.edu.pl/aa/ssss?ssss=2232"));
        
    }

}
