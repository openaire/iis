package eu.dnetlib.iis.common.report;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class ReportEntryJsonAppenderTest {

    private ReportEntryJsonAppender jsonAppender = new ReportEntryJsonAppender();
    
    @Mock
    private ReportValueJsonConverterManager reportValueConverterManager;
    
    
    private JsonParser jsonParser = new JsonParser();
    
    
    private JsonObject jsonReport;
    
    
    @Before
    public void setup() throws IOException {
        
        jsonReport = readJsonFromClasspath("/eu/dnetlib/iis/common/report/report_before.json");
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void appendReportEntry() throws IOException {
        
        // given
        
        ReportEntry reportEntry = new ReportEntry("property1.propertyB.propertyII", ReportEntryType.COUNTER, "21");
        
        when(reportValueConverterManager.convertValue(reportEntry)).thenReturn(new JsonPrimitive(21));
        
        // execute
        
        jsonAppender.appendReportEntry(jsonReport, reportEntry);
        
        // assert
        
        JsonObject expectedJson = readJsonFromClasspath("/eu/dnetlib/iis/common/report/report_after.json");
        
        assertEquals(expectedJson, jsonReport);
        
    }
    
    @Test
    public void appendReportEntry_REPLACE_ALREADY_EXISTING_NODE() throws IOException {
        
        // given
        
        ReportEntry reportEntry = new ReportEntry("property1.propertyB", ReportEntryType.COUNTER, "21");
        
        when(reportValueConverterManager.convertValue(reportEntry)).thenReturn(new JsonPrimitive(21));
        
        // execute
        
        jsonAppender.appendReportEntry(jsonReport, reportEntry);
        
        // assert
        
        JsonObject expectedJson = readJsonFromClasspath("/eu/dnetlib/iis/common/report/report_after_replaced_node.json");
        
        assertEquals(expectedJson, jsonReport);
        
    }
    
    @Test
    public void appendReportEntry_REPLACE_ALREADY_EXISTING_LEAF() throws IOException {

        // given
        
        ReportEntry reportEntry = new ReportEntry("property1.propertyA", ReportEntryType.COUNTER, "21");
        
        when(reportValueConverterManager.convertValue(reportEntry)).thenReturn(new JsonPrimitive(21));
        
        // execute
        
        jsonAppender.appendReportEntry(jsonReport, reportEntry);
        
        // assert
        
        JsonObject expectedJson = readJsonFromClasspath("/eu/dnetlib/iis/common/report/report_after_replaced_leaf.json");
        
        assertEquals(expectedJson, jsonReport);
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private JsonObject readJsonFromClasspath(String jsonClasspath) throws IOException {
        
        try (Reader reader = new InputStreamReader(getClass().getResourceAsStream(jsonClasspath))) {
            JsonElement jsonElement = jsonParser.parse(reader);
            
            return jsonElement.getAsJsonObject();
        }
    }
}
