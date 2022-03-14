package eu.dnetlib.iis.wf.report;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.OOZIE_ACTION_OUTPUT_FILENAME;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AnyPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;

/**
 * Java workflow node responsible for merging partial reports into single json file.<br />
 * It reads input partial report datastores ({@link ReportEntry}s) located under
 * single parent location. Process assumes that partial report datastores are
 * located in subdirectories of the provided input path.<br />
 * As a result process writes merged report into json file. 
 * 
 * @author madryk
 */
public class ReportMerger implements Process {

    protected static final String PARTIAL_REPORTS_PORT_IN_NAME = "partial_reports";
    
    protected static final String REPORT_PORT_OUT_NAME = "report";
    
    
    private final ReportEntryJsonAppender reportEntryAppender = new ReportEntryJsonAppender();
    
    
    //------------------------ LOGIC --------------------------
    
    @Override
    public Map<String, PortType> getInputPorts() {
        return Collections.singletonMap(PARTIAL_REPORTS_PORT_IN_NAME, new AnyPortType());
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        return Collections.singletonMap(REPORT_PORT_OUT_NAME, new AnyPortType());
    }
    
    @Override
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) throws Exception {
        
        FileSystem fs = FileSystem.get(conf);
        
        
        List<ReportEntry> allReportEntries = readAllPartialReports(fs, portBindings.getInput().get(PARTIAL_REPORTS_PORT_IN_NAME));
        
        writeActionData(allReportEntries);
        
        JsonObject jsonReport = buildJsonReport(allReportEntries);
        
        writeJsonReport(jsonReport, fs, portBindings.getOutput().get(REPORT_PORT_OUT_NAME));
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    /**
     * Writes all counters as action data properties.
     */
    private void writeActionData(List<ReportEntry> reportEntries) throws FileNotFoundException, IOException {
        if (CollectionUtils.isNotEmpty(reportEntries)) {
            File file = new File(System.getProperty(OOZIE_ACTION_OUTPUT_FILENAME));
            Properties props = new Properties();
            try (OutputStream os = new FileOutputStream(file)) {
                for (ReportEntry currentEntry : reportEntries) {
                    if (ReportEntryType.COUNTER.equals(currentEntry.getType())) {
                        props.setProperty(currentEntry.getKey().toString(), currentEntry.getValue().toString());    
                    }
                }
                props.store(os, "");
            }
        }
    }
    
    private void writeJsonReport(JsonObject jsonReport, FileSystem fs, Path outputReportPath) throws IOException {
        
        Gson gson = new Gson();
        
        try (JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(fs.create(outputReportPath), "utf8"))) {
            jsonWriter.setIndent("  ");
            gson.toJson(jsonReport, jsonWriter);
            
        }
        
    }
    private JsonObject buildJsonReport(List<ReportEntry> reportEntries) {
        
        JsonObject jsonReport = new JsonObject();
        
        // sorting report entries in order to avoid entries order affecting the JSON structure
        Collections.sort(reportEntries);
        
        for (ReportEntry reportEntry : reportEntries) {
            reportEntryAppender.appendReportEntry(jsonReport, reportEntry);
        }
        
        return jsonReport;
    }
    
    private List<ReportEntry> readAllPartialReports(FileSystem fs, Path partialReportsBasePath) throws FileNotFoundException, IOException {
        
        FileStatus[] reportsBaseDirContent = fs.listStatus(partialReportsBasePath);
        
        List<FileSystemPath> reportDatastorePaths = Lists.newArrayList();
        
        for (FileStatus fileStatus : reportsBaseDirContent) {
            
            if (fs.isDirectory(fileStatus.getPath())) {
                reportDatastorePaths.add(new FileSystemPath(fs, fileStatus.getPath()));
            }
        }
        
        
        List<ReportEntry> allReportEntries = Lists.newArrayList();
        
        for (FileSystemPath datastorePath : reportDatastorePaths) {
            allReportEntries.addAll(DataStore.read(datastorePath, ReportEntry.SCHEMA$));
        }
        
        return allReportEntries;
    }
    

}
