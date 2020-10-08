package eu.dnetlib.iis.wf.report;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.Reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

/**
 * @author madryk
 */
@ExtendWith(MockitoExtension.class)
public class ReportEntryJsonAppenderTest {

    @InjectMocks
    private ReportEntryJsonAppender jsonAppender = new ReportEntryJsonAppender();
    
    @Mock
    private ReportValueJsonConverterManager reportValueConverterManager;
    
    
    private JsonParser jsonParser = new JsonParser();
    
    
    private JsonObject jsonReport;
    
    
    @BeforeEach
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

        try (Reader reader = ClassPathResourceProvider.getResourceReader(jsonClasspath)) {
            JsonElement jsonElement = jsonParser.parse(reader);
            
            return jsonElement.getAsJsonObject();
        }
    }
}
