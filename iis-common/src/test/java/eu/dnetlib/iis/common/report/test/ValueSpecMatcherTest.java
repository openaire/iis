package eu.dnetlib.iis.common.report.test;

import org.junit.jupiter.api.Test;

import static eu.dnetlib.iis.common.report.test.ValueSpecMatcher.EXPRESSION_TYPE_LONG_RANGE;
import static org.junit.jupiter.api.Assertions.*;

/**
* @author Łukasz Dumiszewski
*/

public class ValueSpecMatcherTest {

    private ValueSpecMatcher matcher = new ValueSpecMatcher();
    
    
    //------------------------ TESTS --------------------------
    
        
    @Test
    public void matches_valueSpec_NULL() {
        
        // execute
        assertThrows(NullPointerException.class, () -> matcher.matches("SOME VALUE", null));

    }
    
    @Test
    public void matches_value_NULL_valueSpec_NOT_NULL_EXPRESSION() {
        
        // execute
        assertFalse(matcher.matches(null, "SOME_VALUE_SPEC"));
        
    }
    
    @Test
    public void matches_value_NULL_valueSpec_NULL_EXPRESSION() {
        
        // execute
        assertTrue(matcher.matches(null, "$(NULL)"));
        
    }
    
    @Test
    public void matches_NO_EXPRESSION() {
        
        // execute & assert
        assertTrue(matcher.matches("SOME VALUE", "SOME VALUE"));
        
    }

    @Test
    public void matches_EXPRESSION_UNKNOWN_TYPE() {
        
        // execute & assert
        assertThrows(IllegalArgumentException.class, () ->
                matcher.matches("SOME VALUE", "$(SomeUnknownType: [2, 3])"));

    }
    
    @Test
    public void matches_LONG_RANGE_NOT_DIGITS() {
        
        // execute & assert
        assertThrows(IllegalArgumentException.class, () ->
                matcher.matches("SOME WRONG LONG", createLongRangeExpression("2", "3")));
        
    }
    
    @Test
    public void matches_NOT_A_REAL_EXPRESSION() {
        
        // execute & assert
        assertFalse(matcher.matches("2", createLongRangeExpression("ABC", "3")));
        
    }
    
    @Test
    public void matches() {
        
        // execute & assert
        assertTrue(matcher.matches("3", createLongRangeExpression("2", "4")));
        assertTrue(matcher.matches("3", createLongRangeExpression("2", "4000")));
        assertTrue(matcher.matches("3", createLongRangeExpression("3", "3")));
        assertTrue(matcher.matches("3", createLongRangeExpression("", "4")));
        assertTrue(matcher.matches("3", createLongRangeExpression("2", "")));
        
    }
    
    @Test
    public void matches_NOT() {
        
        // execute & assert
        assertFalse(matcher.matches("3", createLongRangeExpression("4", "70")));
        assertFalse(matcher.matches("3", createLongRangeExpression("4", "")));
        assertFalse(matcher.matches("3", createLongRangeExpression("1", "2")));
        assertFalse(matcher.matches("3", createLongRangeExpression("", "2")));
        
    }

    
    
    //------------------------ PRIVATE --------------------------
    
    private String createLongRangeExpression(String lowerBound, String upperBound) {
        return "$("+EXPRESSION_TYPE_LONG_RANGE+": ["+lowerBound+", "+upperBound+"])";
    }
}
