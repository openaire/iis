package eu.dnetlib.iis.common.report;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.iis.common.counter.PigCounters;

/**
 * Resolver of values with pig counters placeholder.
 * 
 * @author madryk
 */
public class PigCounterValueResolver {

    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\{(.*)\\}");
    
    private static final char PLACEHOLDER_VALUES_SEPARATOR = '.';
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns resolved value using provided {@link PigCounters}.<br/>
     * Passed value can be a simple string or string with placeholder.
     * Simple string will be returned unchanged.<br/>
     * If value contains placeholder it will be resolved to corresponding
     * value from {@link PigCounters}. Placeholder must be in form:
     * <code>{JOB_ALIAS.JOB_COUNTER}</code>.
     */
    public String resolveValue(String value, PigCounters pigCounters) {
        
        Matcher placeholderMatcher = PLACEHOLDER_PATTERN.matcher(value);
        
        if (placeholderMatcher.matches()) {
            
            String[] placeholderValues = StringUtils.split(placeholderMatcher.group(1), PLACEHOLDER_VALUES_SEPARATOR);
            
            if (placeholderValues.length != 2) {
                throw new IllegalArgumentException("Invalid placeholder: " + placeholderMatcher.group(1));
            }
            
            String alias = placeholderValues[0];
            String counterName = placeholderValues[1];
            
            
            String jobId = pigCounters.getJobIdByAlias(alias);
            if (jobId == null) {
                throw new IllegalArgumentException("Invalid job alias: " + alias);
            }
            
            String counterValue = pigCounters.getJobCounters(jobId).getCounter(counterName);
            
            if (StringUtils.isBlank(counterValue)) {
                throw new IllegalArgumentException("Couldn't find a counter with name: " + counterName + " inside job counters, id: " + jobId);
            }
            return counterValue;
        }
        
        return value;
        
    }
    
}
