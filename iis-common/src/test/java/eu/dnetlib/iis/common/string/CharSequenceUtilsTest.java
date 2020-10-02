package eu.dnetlib.iis.common.string;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
