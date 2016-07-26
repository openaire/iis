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

    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\{(.*)\\}");
    
    private static final char PLACEHOLDER_VALUES_SEPARATOR = '.';
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns {@link ReportPigCounterMapping}.
     * 
     * @param destReportCounter - value that defines destination report counter name
     * @param sourcePigCounterName - value that contains details about source pig counter. It must be in form: <code>{JOB_ALIAS.JOB_COUNTER_NAME}</code>
     */
    public ReportPigCounterMapping parse(String destReportCounter, String sourcePigCounterName) {
        Preconditions.checkNotNull(destReportCounter);
        Preconditions.checkNotNull(sourcePigCounterName);
        
        Matcher placeholderMatcher = PLACEHOLDER_PATTERN.matcher(sourcePigCounterName);
        
        if (placeholderMatcher.matches()) {
            
            String[] placeholderValues = StringUtils.split(placeholderMatcher.group(1), PLACEHOLDER_VALUES_SEPARATOR);
            
            if (placeholderValues.length != 2) {
                throw new IllegalArgumentException("Invalid placeholder: " + placeholderMatcher.group(1));
            }
            
            String alias = placeholderValues[0];
            String counterName = placeholderValues[1];
            
            return new ReportPigCounterMapping(destReportCounter, alias, counterName);
        }
        
        throw new IllegalArgumentException("Invalid pig counter name: " + sourcePigCounterName);
    }
}
