package eu.dnetlib.iis.common.report;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import eu.dnetlib.iis.common.utils.AvroTestUtils;

/**
 * @author madryk
 */
public class ReportMergerTest {

    private ReportMerger reportMerger = new ReportMerger();
    
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    
    private JsonParser jsonParser = new JsonParser();
    
    
    private String inputPartialReportsBasePath;
    private String outputReportPath;
    
    
    @Before
    public void setup() throws IOException {
        inputPartialReportsBasePath = tempFolder.newFolder("partial_reports").getPath();
        outputReportPath = tempFolder.getRoot().getPath() + "/report.json";
        
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
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private JsonObject readJson(String jsonPath) throws FileNotFoundException, IOException {
        
        try (Reader reader = new FileReader(jsonPath)) {
            JsonElement jsonElement = jsonParser.parse(reader);
            
            return jsonElement.getAsJsonObject();
        }
    }
    
    private JsonObject readJsonFromClasspath(String jsonClasspath) throws IOException {
        
        try (Reader reader = new InputStreamReader(getClass().getResourceAsStream(jsonClasspath))) {
            JsonElement jsonElement = jsonParser.parse(reader);
            
            return jsonElement.getAsJsonObject();
        }
    }
    
}
