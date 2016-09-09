package eu.dnetlib.iis.common.report;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * @author madryk
 */
public class ReportFileNameGeneratorTest {
    
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    
    private File propertiesFile; 
    
    
    @Before
    public void setup() throws IOException {
        propertiesFile = tempFolder.newFile("action_output.properties");
        
        System.setProperty("oozie.action.output.properties", propertiesFile.getAbsolutePath());
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = RuntimeException.class)
    public void test_NO_WORKFLOW_NAME_ARGUMENT() throws Exception {
        
        // given
        String[] args = new String[] { };
        
        // execute
        ReportFileNameGenerator.main(args);
        
    }
    
    @Test
    public void test() throws Exception {
        
        // given
        
        String[] args = new String[] { "wf_name" };
        
        // execute
        
        long dateBefore = fetchDateWithoutSeconds();
        
        ReportFileNameGenerator.main(args);
        
        long dateAfter = fetchDateWithoutSeconds();
        
        
        // assert
        
        Properties actualProperties = new Properties();
        try (InputStream is = new FileInputStream(propertiesFile)) {
            
            actualProperties.load(is);
        }
        assertEquals(1, actualProperties.size());
        
        String actualReportFilename = actualProperties.getProperty("report_filename");
        
        assertNotNull(actualReportFilename);
        assertTrue(actualReportFilename.matches("wf_name_report_\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}.json"));
        
        String filenameDatePart = actualReportFilename.substring("wf_name_report_".length(), actualReportFilename.length() - ".json".length());
        
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        
        Date actualDate = dateFormat.parse(filenameDatePart);
        
        assertTrue(actualDate.getTime() >= dateBefore);
        assertTrue(actualDate.getTime() <= dateAfter);
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private long fetchDateWithoutSeconds() {
        
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        
        return calendar.getTimeInMillis();
    }
}
