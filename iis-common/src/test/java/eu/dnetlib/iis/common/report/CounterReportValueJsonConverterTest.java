package eu.dnetlib.iis.common.report;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;

/**
 * @author madryk
 */
public class CounterReportValueJsonConverterTest {

    private CounterReportValueJsonConverter converter = new CounterReportValueJsonConverter();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void isApplicable() {
        
        // execute & assert
        
        assertTrue(converter.isApplicable(ReportEntryType.COUNTER));
        assertFalse(converter.isApplicable(ReportEntryType.DURATION));
        
    }
    
    @Test
    public void convertValue() {
        
        // given
        
        ReportEntry reportEntry = new ReportEntry("report.key", ReportEntryType.COUNTER, "45");
        
        // execute
        
        JsonElement json = converter.convertValue(reportEntry);
        
        
        // assert
        
        assertTrue(json instanceof JsonPrimitive);
        assertEquals(45L, json.getAsLong());
        
    }
}
