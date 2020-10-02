package eu.dnetlib.iis.common.report;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author madryk
 */
public class ReportEntryFactoryTest {

    
    //------------------------ TESTS --------------------------
    
    @Test
    public void createCounterReportParam() {
        
        // execute
        
        ReportEntry reportEntry = ReportEntryFactory.createCounterReportEntry("report.key", 43);
        
        // assert
        
        assertEquals("report.key", reportEntry.getKey());
        assertEquals(ReportEntryType.COUNTER, reportEntry.getType());
        assertEquals("43", reportEntry.getValue());
    }
    
    @Test
    public void createDurationReportParam() {
        
        // execute
        
        ReportEntry reportEntry = ReportEntryFactory.createDurationReportEntry("report.key", 3600000);
        
        // assert
        
        assertEquals("report.key", reportEntry.getKey());
        assertEquals(ReportEntryType.DURATION, reportEntry.getType());
        assertEquals("3600000", reportEntry.getValue());
    }
}
