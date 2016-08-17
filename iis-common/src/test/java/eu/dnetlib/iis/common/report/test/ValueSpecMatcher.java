package eu.dnetlib.iis.common.report.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import com.google.common.base.Preconditions;


/**
 * A matcher that decides whether a string value matches a given value specification.<br/>
 * The value specification may be a string value itself or an expression that defines the expected value.<br/>
 * The value specification is considered to be an expression if it is in parentheses preceded by $.<br/>
 * There is one special expression declaring that the value compared should be null: $(NULL). Other expressions consist
 * of the type of the operation and the expected values.<br/><br/>
 * At present only one expression type is supported: LONG_RANGE.<br/>
 * Examples:<br/>
 * <code>
 * $(LONG_RANGE: [0, 100]) - the value must be a number between 0 and 100 (inclusively)<br/>
 * $(LONG_RANGE: [2, ]) - the value must be greater than 2<br/>
 * $(LONG_RANGE: [, 5]) - the value must be less than 5
 * </code>
 * 
 *  
 * 
 * @author Łukasz Dumiszewski
*/

public class ValueSpecMatcher {

    
    public static final String EXPRESSION_TYPE_LONG_RANGE = "LONG_RANGE";

    private String NULL_VALUE_PATTERN = "$(NULL)";
    private Pattern VALUE_EXPRESSION_PATTERN = Pattern.compile("^\\$\\((\\w+):\\s*\\[(\\d*)\\,\\s*(\\d*)\\]\\)$");
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Says whether the given value matches the given valueSpecification.
     * See the description of the class for the format of value specifications.<br/><br/>
     * Examples:<br/>
     * <code><br/>
     * matches("34", "34") => true<br/>
     * matches("34", "35") => false<br/>
     * matches(null, "$(NULL)") => true<br/>
     * matches(null, "") => false<br/>
     * matches("34", "$(LONG_RANGE: [32, 36])") => true<br/>
     * matches("34", "$(LONG_RANGE: [34, 34])") => true<br/>
     * matches("34", "$(LONG_RANGE: [30, ])") => true<br/>
     * matches("34", "$(LONG_RANGE: [, 34])") => true<br/>
     * matches("35", "$(LONG_RANGE: [, 34])") => false<br/>
     * matches("39", "$(LONG_RANGE: [34, 38])") => false<br/>
     * </code>
     *  
     * 
     */
    public boolean matches(String value, String valueSpecification) {
        
        Preconditions.checkNotNull(valueSpecification);
        
        if (value == null) {
            return valueSpecification.equals(NULL_VALUE_PATTERN);
        }
        
        Matcher expressionMatcher = VALUE_EXPRESSION_PATTERN.matcher(valueSpecification);
        
        
        if (!expressionMatcher.matches()) {
        
            return value.equals(valueSpecification);
        
        }
        
       
        String valueExpType = expressionMatcher.group(1);
        
        if (valueExpType.equals(EXPRESSION_TYPE_LONG_RANGE)) {
            
            return matchesLongRange(value, expressionMatcher);
        }
        
        throw new IllegalArgumentException("The value expression has unknown type " + valueSpecification);
        
    }

    
    //------------------------ PRIVATE --------------------------

    private boolean matchesLongRange(String actualValue, Matcher expressionMatcher) {

        if (!NumberUtils.isDigits(actualValue)) {
            throw new IllegalArgumentException("The expected value type is " + EXPRESSION_TYPE_LONG_RANGE + ", but the actual value is not a LONG number");
        }

        long actualLong = Long.parseLong(actualValue);
        long lowerBound = actualLong - 1;
        if (StringUtils.isNotBlank(expressionMatcher.group(2))) {
            lowerBound = Long.parseLong(expressionMatcher.group(2));
        };
        long upperBound =actualLong + 1;
        if (StringUtils.isNotBlank(expressionMatcher.group(3))) {
            upperBound = Long.parseLong(expressionMatcher.group(3));
        }
        
        return actualLong >= lowerBound && actualLong <= upperBound;
    }
    
}
