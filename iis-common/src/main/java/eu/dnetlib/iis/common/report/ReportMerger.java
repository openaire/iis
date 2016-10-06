package eu.dnetlib.iis.common.report;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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

    private static final String PARTIAL_REPORTS_PORT_IN_NAME = "partial_reports";
    
    private static final String REPORT_PORT_OUT_NAME = "report";
    
    
    private ReportEntryJsonAppender reportEntryAppender = new ReportEntryJsonAppender();
    
    
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
        
        JsonObject jsonReport = buildJsonReport(allReportEntries);
        
        writeJsonReport(jsonReport, fs, portBindings.getOutput().get(REPORT_PORT_OUT_NAME));
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void writeJsonReport(JsonObject jsonReport, FileSystem fs, Path outputReportPath) throws IOException {
        
        Gson gson = new Gson();
        
        try (JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(fs.create(outputReportPath)))) {
            jsonWriter.setIndent("  ");
            gson.toJson(jsonReport, jsonWriter);
            
        }
        
    }
    private JsonObject buildJsonReport(List<ReportEntry> reportEntries) {
        
        JsonObject jsonReport = new JsonObject();
        
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
