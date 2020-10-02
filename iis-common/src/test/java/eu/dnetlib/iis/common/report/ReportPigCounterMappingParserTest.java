package eu.dnetlib.iis.common.report;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author madryk
 */
public class ReportPigCounterMappingParserTest {

    public ReportPigCounterMappingParser reportPigCounterMappingParser = new ReportPigCounterMappingParser();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void parse_destCounter_NULL() {
        
        // execute
        assertThrows(NullPointerException.class, () ->
                reportPigCounterMappingParser.parse(null, "{jobAlias.COUNTER_NAME}"));
    }
    
    @Test
    public void parse_sourceCounter_NULL() {
        
        // execute
        assertThrows(NullPointerException.class, () ->
                reportPigCounterMappingParser.parse("destination.report.counter", null));
    }
    
    @Test
    public void parse_sourceCounter_INVALID_PLACEHOLDER() {
        
        // execute
        assertThrows(IllegalArgumentException.class, () ->
                reportPigCounterMappingParser.parse("destination.report.counter", "jobAlias.COUNTER_NAME"));
    }
    
    @Test
    public void parse_sourceCounter_INVALID_PLACEHOLDER_2() {
        
        // execute
        assertThrows(IllegalArgumentException.class, () ->
                reportPigCounterMappingParser.parse("destination.report.counter", "COUNTER_NAME"));
        
    }
    
    @Test
    public void parse_sourceCounter_INVALID_PLACEHOLDER_TOO_MANY_LEVELS() {
        
        // execute
        assertThrows(IllegalArgumentException.class, () ->
                reportPigCounterMappingParser.parse("destination.report.counter", "{jobAlias.COUNTER_NAME.SUB_COUNTER}"));
        
    }
    
    
    @Test
    public void parse_JOB_LEVEL_PIG_COUNTER() {
        
        // execute
        
        ReportPigCounterMapping mapping = reportPigCounterMappingParser.parse("destination.report.counter", "{jobAlias.COUNTER_NAME}");
        
        // assert
        
        assertEquals("destination.report.counter", mapping.getDestReportCounterName());
        assertEquals("jobAlias", mapping.getSourcePigJobAlias());
        assertEquals("COUNTER_NAME", mapping.getSourcePigCounterName());
        
    }
    
    @Test
    public void parse_ROOT_LEVEL_PIG_COUNTER() {
        
        // execute
        
        ReportPigCounterMapping mapping = reportPigCounterMappingParser.parse("destination.report.counter", "{COUNTER_NAME}");
        
        // assert
        
        assertEquals("destination.report.counter", mapping.getDestReportCounterName());
        assertNull(mapping.getSourcePigJobAlias());
        assertEquals("COUNTER_NAME", mapping.getSourcePigCounterName());
        
    }
}
