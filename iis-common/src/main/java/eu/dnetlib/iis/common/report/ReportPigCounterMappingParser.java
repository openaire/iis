package eu.dnetlib.iis.common.report;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

/**
 * Parser of {@link ReportPigCounterMapping}.
 * 
 * @author madryk
 */
public class ReportPigCounterMappingParser {

    private static final Pattern PIG_COUNTER_PLACEHOLDER_PATTERN = Pattern.compile("\\{(.*)\\}");
    
    private static final char PLACEHOLDER_VALUES_SEPARATOR = '.';
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns {@link ReportPigCounterMapping}.
     * 
     * @param destReportCounter - value that defines destination report counter name
     * @param sourcePigCounterName - value that contains details about source pig counter. 
     * It must be in form: <code>{JOB_ALIAS.JOB_COUNTER_NAME}</code> for job level counters
     * OR <code>{COUNTER_NAME}</code> for root level counters
     */
    public ReportPigCounterMapping parse(String destReportCounter, String sourcePigCounterName) {
        
        Preconditions.checkNotNull(destReportCounter);
        Preconditions.checkNotNull(sourcePigCounterName);
        
        
        Matcher placeholderMatcher = PIG_COUNTER_PLACEHOLDER_PATTERN.matcher(sourcePigCounterName);
        
        if (!placeholderMatcher.matches()) {
        
            throw new IllegalArgumentException("invalid pig counter placeholder:" + sourcePigCounterName);
        
        }
        
            
        String[] placeholderValues = StringUtils.split(placeholderMatcher.group(1), PLACEHOLDER_VALUES_SEPARATOR);
        
        
        if (placeholderValues.length == 1) { // root level pig counter
            
            return new ReportPigCounterMapping(placeholderValues[0], null, destReportCounter);
        
        }
        
        if (placeholderValues.length == 2) { // job level pig counter
            
            String alias = placeholderValues[0];
            String counterName = placeholderValues[1];
            
            return new ReportPigCounterMapping(counterName, alias, destReportCounter);
            
        } 
        
        throw new IllegalArgumentException("Invalid placeholder, to many levels (dots): " + placeholderMatcher.group(1));
            
        
    }
}
