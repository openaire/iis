package eu.dnetlib.iis.common.report;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.dnetlib.iis.common.schemas.ReportParam;

/**
 * @author madryk
 */
public class ReportParamJsonAppenderTest {

    private ReportParamJsonAppender jsonAppender = new ReportParamJsonAppender();
    
    private JsonParser jsonParser = new JsonParser();
    
    
    private JsonObject jsonReport;
    
    
    @Before
    public void setup() throws IOException {
        
        jsonReport = readJsonFromClasspath("/eu/dnetlib/iis/common/report/report_before.json");
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void appendReportParam() throws IOException {
        
        // given
        
        ReportParam reportParam = new ReportParam("property1.propertyB.propertyII", "21");
        
        // execute
        
        jsonAppender.appendReportParam(jsonReport, reportParam);
        
        // assert
        
        JsonObject expectedJson = readJsonFromClasspath("/eu/dnetlib/iis/common/report/report_after.json");
        
        assertEquals(expectedJson, jsonReport);
        
    }
    
    @Test
    public void appendReportParam_REPLACE_ALREADY_EXISTING_NODE() throws IOException {
        
        // given
        
        ReportParam reportParam = new ReportParam("property1.propertyB", "21");
        
        // execute
        
        jsonAppender.appendReportParam(jsonReport, reportParam);
        
        // assert
        
        JsonObject expectedJson = readJsonFromClasspath("/eu/dnetlib/iis/common/report/report_after_replaced_node.json");
        
        assertEquals(expectedJson, jsonReport);
        
    }
    
    @Test
    public void appendReportParam_REPLACE_ALREADY_EXISTING_LEAF() throws IOException {

        // given
        
        ReportParam reportParam = new ReportParam("property1.propertyA", "21");
        
        // execute
        
        jsonAppender.appendReportParam(jsonReport, reportParam);
        
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
