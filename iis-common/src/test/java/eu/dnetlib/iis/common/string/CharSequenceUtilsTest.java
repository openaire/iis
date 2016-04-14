package eu.dnetlib.iis.common.string;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
* @author Łukasz Dumiszewski
*/

public class CharSequenceUtilsTest {

    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void toStringWithNullToEmpty() {
    
        // given
        
        StringBuilder sb = new StringBuilder();
        sb.append("MMMM");
        
        
        // execute & assert
        
        assertEquals("MMMM", CharSequenceUtils.toStringWithNullToEmpty(sb));
        
    }
    
    
    @Test
    public void toStringWithNullToEmpty_NULL() {
    
        // execute & assert
        
        assertEquals("", CharSequenceUtils.toStringWithNullToEmpty(null));
        
    }
    
}
