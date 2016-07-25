package eu.dnetlib.iis.common.report;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * @author madryk
 */
public class ReportPigCounterMappingParserTest {

    public ReportPigCounterMappingParser reportPigCounterMappingParser = new ReportPigCounterMappingParser();
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void parse_NULL_DEST_REPORT_COUNTER() {
        
        // execute
        reportPigCounterMappingParser.parse(null, "{jobAlias.COUNTER_NAME}");
    }
    
    @Test(expected = NullPointerException.class)
    public void parse_NULL_SOURCE_PIG_COUNTER() {
        
        // execute
        reportPigCounterMappingParser.parse("destination.report.counter", null);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void parse_NO_PLACEHOLDER() {
        
        // execute
        reportPigCounterMappingParser.parse("destination.report.counter", "jobAlias.COUNTER_NAME");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void parse_INVALID_PLACEHOLDER() {
        
        // execute
        reportPigCounterMappingParser.parse("destination.report.counter", "COUNTER_NAME");
        
    }
    
    @Test
    public void parse() {
        
        // execute
        
        ReportPigCounterMapping mapping = reportPigCounterMappingParser.parse("destination.report.counter", "{jobAlias.COUNTER_NAME}");
        
        // assert
        
        assertEquals("destination.report.counter", mapping.getDestReportCounterName());
        assertEquals("jobAlias", mapping.getSourcePigJobAlias());
        assertEquals("COUNTER_NAME", mapping.getSourcePigJobCounterName());
        
    }
}
