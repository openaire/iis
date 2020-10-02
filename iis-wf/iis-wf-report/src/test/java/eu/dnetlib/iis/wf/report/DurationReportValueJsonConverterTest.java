package eu.dnetlib.iis.wf.report;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author madryk
 */
public class DurationReportValueJsonConverterTest {

    private DurationReportValueJsonConverter converter = new DurationReportValueJsonConverter();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void isApplicable() {
        
        // execute & assert
        
        assertTrue(converter.isApplicable(ReportEntryType.DURATION));
        assertFalse(converter.isApplicable(ReportEntryType.COUNTER));
        
    }
    
    @Test
    public void convertValue() {
        
        // given
        
        ReportEntry reportEntry = new ReportEntry("report.key", ReportEntryType.DURATION, "3842000");
        
        // execute
        
        JsonElement json = converter.convertValue(reportEntry);
        
        
        // assert
        
        JsonElement expectedJson = new JsonParser().parse("{milliseconds: 3842000, humanReadable: \"1h 04m 02s\"}");
        
        assertEquals(expectedJson, json);
        
    }
}
