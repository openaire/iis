package eu.dnetlib.iis.wf.report;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.porttype.AnyPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.OOZIE_ACTION_OUTPUT_FILENAME;
import static eu.dnetlib.iis.wf.report.ReportMerger.PARTIAL_REPORTS_PORT_IN_NAME;
import static eu.dnetlib.iis.wf.report.ReportMerger.REPORT_PORT_OUT_NAME;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author madryk
 */
public class ReportMergerTest {

    private ReportMerger reportMerger = new ReportMerger();

    @TempDir
    public File tempFolder;
    
    private JsonParser jsonParser = new JsonParser();
    
    
    private String inputPartialReportsBasePath;
    private String outputReportPath;
    
    
    @BeforeEach
    public void setup() throws IOException {
        inputPartialReportsBasePath = Files.createDirectory(tempFolder.toPath().resolve("partial_reports")).toFile().getPath();
        outputReportPath = tempFolder.getPath() + "/report.json";
        System.setProperty(OOZIE_ACTION_OUTPUT_FILENAME, 
                tempFolder.getAbsolutePath() + File.separatorChar + "test.properties");
        
    }
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void run() throws Exception {
        
        // given
        
        List<ReportEntry> partialReport1 = Lists.newArrayList(
                new ReportEntry("param1.paramA.I", ReportEntryType.COUNTER, "3"), 
                new ReportEntry("param1.paramA.III", ReportEntryType.COUNTER, "6"), 
                new ReportEntry("param1.paramB", ReportEntryType.COUNTER, "4"),
                new ReportEntry("param1.paramX", ReportEntryType.COUNTER, "33"));
        List<ReportEntry> partialReport2 = Lists.newArrayList(
                new ReportEntry("param2", ReportEntryType.COUNTER, "12"), 
                new ReportEntry("param1.paramA.II", ReportEntryType.COUNTER, "2"),
                new ReportEntry("param1.paramC.II", ReportEntryType.COUNTER, "5"),
                new ReportEntry("param1.paramA.duration", ReportEntryType.DURATION, "1000"),
                new ReportEntry("param1.paramX.Z", ReportEntryType.COUNTER, "1333"));
        
        AvroTestUtils.createLocalAvroDataStore(partialReport1, inputPartialReportsBasePath + "/report1");
        AvroTestUtils.createLocalAvroDataStore(partialReport2, inputPartialReportsBasePath + "/report2");
        
        PortBindings portBindings = new PortBindings(
                ImmutableMap.of("partial_reports", new Path(inputPartialReportsBasePath)), 
                ImmutableMap.of("report", new Path(outputReportPath)));
        Configuration conf = new Configuration(false);
        
        // execute
        
        reportMerger.run(portBindings, conf, Collections.emptyMap());
        
        
        // assert
        
        JsonObject actualReportJson = readJson(outputReportPath);
        
        JsonObject expectedJsonReport = readJsonFromClasspath("/eu/dnetlib/iis/common/report/report_merged.json");
        
        assertEquals(expectedJsonReport, actualReportJson);
        
        Properties actionData = getStoredProperties();
        assertNotNull(actionData);
        assertEquals(8, actionData.size());
        assertEquals("3", actionData.getProperty("param1.paramA.I"));
        assertEquals("6", actionData.getProperty("param1.paramA.III"));
        assertEquals("4", actionData.getProperty("param1.paramB"));
        assertEquals("33", actionData.getProperty("param1.paramX"));
        assertEquals("12", actionData.getProperty("param2"));
        assertEquals("2", actionData.getProperty("param1.paramA.II"));
        assertEquals("5", actionData.getProperty("param1.paramC.II"));
        assertEquals("1333", actionData.getProperty("param1.paramX.Z"));
    }
    
    @Test
    public void testGetInputPorts() throws Exception {
        // execute
        Map<String, PortType> result = reportMerger.getInputPorts();
        
        // assert
        assertNotNull(result);
        assertNotNull(result.get(PARTIAL_REPORTS_PORT_IN_NAME));
        assertTrue(result.get(PARTIAL_REPORTS_PORT_IN_NAME) instanceof AnyPortType);
    }
    
    @Test
    public void testGetOutputPorts() throws Exception {
        // execute
        Map<String, PortType> result = reportMerger.getOutputPorts();
        
        // assert
        assertNotNull(result);
        assertNotNull(result.get(REPORT_PORT_OUT_NAME));
        assertTrue(result.get(REPORT_PORT_OUT_NAME) instanceof AnyPortType);
    }
    
    //------------------------ PRIVATE --------------------------
    
    private JsonObject readJson(String jsonPath) throws FileNotFoundException, IOException {
        
        try (Reader reader = new FileReader(jsonPath)) {
            JsonElement jsonElement = jsonParser.parse(reader);
            
            return jsonElement.getAsJsonObject();
        }
    }
    
    private JsonObject readJsonFromClasspath(String jsonClasspath) throws IOException {

        try (Reader reader = ClassPathResourceProvider.getResourceReader(jsonClasspath)) {
            JsonElement jsonElement = jsonParser.parse(reader);
            
            return jsonElement.getAsJsonObject();
        }
    }
    
    private Properties getStoredProperties() throws FileNotFoundException, IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(System.getProperty(OOZIE_ACTION_OUTPUT_FILENAME)));
        return properties;
    }
    
}
