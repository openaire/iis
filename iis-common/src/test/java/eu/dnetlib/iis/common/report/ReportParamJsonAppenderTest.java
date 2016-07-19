package eu.dnetlib.iis.common.report;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

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
    public void setup() {
        
        String json = "{"
                + "  property1: {"
                + "    propertyA: 1,"
                + "    propertyB: {"
                + "      propertyI: 9"
                + "    }"
                + "  },"
                + "  property2: 3"
                + "}";
        
        
        jsonReport = jsonParser.parse(json).getAsJsonObject();
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void appendReportParam() {
        
        // given
        
        ReportParam reportParam = new ReportParam("property1.propertyB.propertyII", "21");
        
        // execute
        
        jsonAppender.appendReportParam(jsonReport, reportParam);
        
        // assert
        
        String expectedJson = "{"
                + "  property1: {"
                + "    propertyA: 1,"
                + "    propertyB: {"
                + "      propertyI: 9,"
                + "      propertyII: 21"
                + "    }"
                + "  },"
                + "  property2: 3"
                + "}";
        
        assertEquals(jsonParser.parse(expectedJson), jsonReport);
        
    }
    
    @Test
    public void appendReportParam_REPLACE_ALREADY_EXISTING_NODE() {
        
        // given
        
        ReportParam reportParam = new ReportParam("property1.propertyB", "21");
        
        // execute
        
        jsonAppender.appendReportParam(jsonReport, reportParam);
        
        // assert
        
        String expectedJson = "{"
                + "  property1: {"
                + "    propertyA: 1,"
                + "    propertyB: 21"
                + "  },"
                + "  property2: 3"
                + "}";
        
        assertEquals(jsonParser.parse(expectedJson), jsonReport);
        
    }
    
    @Test
    public void appendReportParam_REPLACE_ALREADY_EXISTING_LEAF() {

        // given
        
        ReportParam reportParam = new ReportParam("property1.propertyA", "21");
        
        // execute
        
        jsonAppender.appendReportParam(jsonReport, reportParam);
        
        // assert
        
        String expectedJson = "{"
                + "  property1: {"
                + "    propertyA: 21,"
                + "    propertyB: {"
                + "      propertyI: 9"
                + "    }"
                + "  },"
                + "  property2: 3"
                + "}";
        
        assertEquals(jsonParser.parse(expectedJson), jsonReport);
        
    }
}
