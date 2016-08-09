package eu.dnetlib.iis.common.report;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.common.oozie.OozieClientFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;

/**
 * Java workflow node process that builds execution time report.<br/>
 * It can report duration of oozie actions that were executed in
 * specified workflow job.<br/>
 * The process needs <code>oozieServiceLoc</code> and <code>jobId</code>
 * properties to successfully connect to oozie and fetch workflow job actions.<br/>
 * The process writes the built report into an avro datastore of {@link ReportEntry}s
 * at the location specified by the output port.<br/>
 * 
 * @author madryk
 */
public class OozieTimeReportGenerator implements Process {

    private static final String REPORT_PORT_OUT_NAME = "report";
    
    private static final String WORKFLOW_JOB_ID_PARAM = "jobId";
    
    private static final String OOZIE_SERVICE_LOC_PARAM = "oozieServiceLoc";
    
    private static final String REPORT_PROPERTY_PREFIX = "report.";
    
    
    private OozieClientFactory oozieClientFactory = new OozieClientFactory();

    
    //------------------------ LOGIC --------------------------
    
    @Override
    public Map<String, PortType> getInputPorts() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        return Collections.singletonMap(REPORT_PORT_OUT_NAME, new AvroPortType(ReportEntry.SCHEMA$));
    }
    
    @Override
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) throws Exception {
        
        List<WorkflowAction> actions = fetchWorkflowActions(parameters.get(OOZIE_SERVICE_LOC_PARAM), parameters.get(WORKFLOW_JOB_ID_PARAM));
        
        Map<String, List<String>> reportKeysToActionNames = mapReportKeysToActionNames(parameters);
        
        
        List<ReportEntry> reportEntries = Lists.newArrayList();
        
        for (Map.Entry<String, List<String>> reportKeyToActionNamesEntry : reportKeysToActionNames.entrySet()) {
            
            long totalDuration = 0L;
            
            for (String actionName : reportKeyToActionNamesEntry.getValue()) {
                
                totalDuration += fetchActionDuration(actions, actionName);
            }
            
            reportEntries.add(ReportEntryFactory.createDurationReportEntry(reportKeyToActionNamesEntry.getKey(), totalDuration));
        }
        
        
        
        FileSystem fs = FileSystem.get(conf);
        
        Path reportPath = portBindings.getOutput().get(REPORT_PORT_OUT_NAME);
        
        DataStore.create(reportEntries, new FileSystemPath(fs, reportPath));
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private List<WorkflowAction> fetchWorkflowActions(String oozieUrl, String workflowId) throws OozieClientException {
        
        OozieClient oozieClient = oozieClientFactory.createOozieClient(oozieUrl);
        
        WorkflowJob job = oozieClient.getJobInfo(workflowId);
        
        return job.getActions();
    }
    
    private long fetchActionDuration(List<WorkflowAction> actions, String actionName) {
        
        for (WorkflowAction action : actions) {
            
            if (actionName.equals(action.getName())) {
                
                Date startDate = action.getStartTime();
                Date endDate = action.getEndTime();
                
                return endDate.getTime() - startDate.getTime();
            }
            
        }
        
        throw new IllegalArgumentException("no action with the name specified: " + actionName);
    }
    
    private Map<String, List<String>> mapReportKeysToActionNames(Map<String, String> parameters) {
        
        return parameters.entrySet().stream()
                .filter(property -> property.getKey().startsWith(REPORT_PROPERTY_PREFIX))
                .map(x -> Pair.of(x.getKey().substring(REPORT_PROPERTY_PREFIX.length()), extractActionNames(x.getValue())))
                .collect(Collectors.toMap(e -> e.getLeft(), e -> e.getRight()));
        
    }
    
    private List<String> extractActionNames(String actionNames) {
        return Lists.newArrayList(StringUtils.split(actionNames, ','));
    }
}
