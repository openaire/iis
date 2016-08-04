package eu.dnetlib.iis.common.report;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * @author madryk
 */
public class ReportPigCounterMappingParserTest {

    public ReportPigCounterMappingParser reportPigCounterMappingParser = new ReportPigCounterMappingParser();
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void parse_destCounter_NULL() {
        
        // execute
        reportPigCounterMappingParser.parse(null, "{jobAlias.COUNTER_NAME}");
    }
    
    @Test(expected = NullPointerException.class)
    public void parse_sourceCounter_NULL() {
        
        // execute
        reportPigCounterMappingParser.parse("destination.report.counter", null);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void parse_sourceCounter_INVALID_PLACEHOLDER() {
        
        // execute
        reportPigCounterMappingParser.parse("destination.report.counter", "jobAlias.COUNTER_NAME");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void parse_sourceCounter_INVALID_PLACEHOLDER_2() {
        
        // execute
        reportPigCounterMappingParser.parse("destination.report.counter", "COUNTER_NAME");
        
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void parse_sourceCounter_INVALID_PLACEHOLDER_TOO_MANY_LEVELS() {
        
        // execute
        reportPigCounterMappingParser.parse("destination.report.counter", "{jobAlias.COUNTER_NAME.SUB_COUNTER}");
        
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
