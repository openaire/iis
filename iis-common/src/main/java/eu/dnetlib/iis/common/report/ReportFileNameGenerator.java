package eu.dnetlib.iis.common.report;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.cli.ParseException;

/**
 * Generator of report filename for given workflow.
 * Generated filename is based on current date:
 * <code>[workflow_name]_report_[yyyy-MM-dd_HH-mm].json</code>
 * 
 * @author madryk
 */
public class ReportFileNameGenerator {

    private static final String REPORT_FILENAME_PROPERTY = "report_filename";
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
        
        if (args.length == 0) {
            throw new RuntimeException("Workflow name must be provided as first argument");
        }
        
        String workflowName = args[0];
        String dateString = generateDateString();
        
        String reportFilename = workflowName + "_report_" + dateString + ".json";
        
        File file = new File(System.getProperty("oozie.action.output.properties"));
        
        
        saveReportFileNameProperties(file, reportFilename);
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private static String generateDateString() {
        
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        
        return dateFormat.format(new Date());
    }
    
    private static void saveReportFileNameProperties(File propertiesFile, String reportFilename) throws FileNotFoundException, IOException {
        
        Properties props = new Properties();
        props.put(REPORT_FILENAME_PROPERTY, reportFilename);
        
        try (OutputStream os = new FileOutputStream(propertiesFile)) {
            
            props.store(os, null);
            
        }
    }
}
