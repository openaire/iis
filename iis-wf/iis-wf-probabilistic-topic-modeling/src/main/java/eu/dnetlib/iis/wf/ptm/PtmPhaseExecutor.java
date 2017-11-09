package eu.dnetlib.iis.wf.ptm;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.ptm.service.Command;
import eu.dnetlib.ptm.service.JobStatus;
import eu.dnetlib.ptm.service.PtmService;
import eu.dnetlib.ptm.service.ExecutionReport;

/**
 * Generic PTM phase executor. Executes given PTM phase with set of parameters provided at input.
 * 
 * @author mhorst
 *
 */
public class PtmPhaseExecutor implements Process {

    
    protected static final String PTM_PARAM_PREFIX = "ptm.";
    
    protected static final String CHECK_STATUS_INTERVAL_SECS_PARAM = "checkStatusIntevalSecs";
    
    protected static final String SERVICE_LOCATION_PARAM = "ptmServiceLocation";
    
    protected static final String PHASE_PARAM = "phase";

    protected static final String PHASE_NAME_ANNOTATE = "annotate";
    
    protected static final String PHASE_NAME_TOPIC_MODELING = "topic-modeling";

    private static final int DEFAULT_CHECK_STATUS_INTERVAL_SECS = 60;
    
    
    private final Logger log = Logger.getLogger(this.getClass());
    
    
    @Override
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) throws Exception {
        
        String serviceLocation = parameters.get(SERVICE_LOCATION_PARAM);
        String phaseName = parameters.get(PHASE_PARAM);
        
        Preconditions.checkNotNull(serviceLocation, "'" + SERVICE_LOCATION_PARAM + "' parameter is required!");
        Preconditions.checkNotNull(phaseName, "'" + PHASE_PARAM + "' parameter is required!");

        long checkStatusInterval = parameters.containsKey(CHECK_STATUS_INTERVAL_SECS_PARAM)
                ? Integer.parseInt(parameters.get(CHECK_STATUS_INTERVAL_SECS_PARAM)) * 1000 : DEFAULT_CHECK_STATUS_INTERVAL_SECS * 1000;
        
        PtmService serviceFacade = buildServiceFacade(serviceLocation);
                
        String jobId;
                
        switch (phaseName) {
        case PHASE_NAME_ANNOTATE: {
            jobId = serviceFacade.annotate(new Command(extractPtmInternalParams(parameters)));
            break;
        }
        case PHASE_NAME_TOPIC_MODELING: {
            jobId = serviceFacade.topicModeling(new Command(extractPtmInternalParams(parameters)));
            break;
        }
        default: {
            throw new RuntimeException("unsupported phase name: " + phaseName);
        }
        }
        
        ExecutionReport report;
        
        log.info("job started: " + jobId);
        
        while (JobStatus.ongoing.equals((report = serviceFacade.getReport(jobId)).getStatus())) {
            log.debug("awaiting '" + phaseName + "' PTM phase to finish...");
            Thread.sleep(checkStatusInterval);
        }
        
        log.info("got final job status: " + report.getStatus());
        
        if (!JobStatus.succeeded.equals(report.getStatus())) {
            throw new RuntimeException(buildMessage(jobId, report));
        }
    }

    @Override
    public Map<String, PortType> getInputPorts() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        return Collections.emptyMap();
    }
    
    protected PtmService buildServiceFacade(String serviceLocation) {
        return new PtmServiceFacade(serviceLocation);
    }
    
    // ------------------------------- PRIVATE ----------------------------------
    
    private String buildMessage(String jobId, ExecutionReport report) {
        StringBuffer errorMessage = new StringBuffer("PTM job " + jobId + "' has ended with status: " + report.getStatus());
        errorMessage.append('\n');
        errorMessage.append("set of input parameters: " + report.getProperties());

        if (report.getErrorDetails() != null) {
            errorMessage.append('\n');
            errorMessage.append("error message: ");
            errorMessage.append(report.getErrorDetails().getMessage());
            errorMessage.append('\n');
            errorMessage.append("error stacktrace: ");
            errorMessage.append(report.getErrorDetails().getStackTrace());
        }
        
        return errorMessage.toString();
    }
    
    private static Map<String,String> extractPtmInternalParams(Map<String, String> parameters) {
        Map<String,String> ptmInternalParams = new HashMap<>();
        for (Entry<String, String> entry : parameters.entrySet()) {
            if (entry.getKey().startsWith(PTM_PARAM_PREFIX) && 
                    !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(entry.getValue())) {
                ptmInternalParams.put(entry.getKey().substring(PTM_PARAM_PREFIX.length()), entry.getValue());
            }
        }
        return ptmInternalParams;
    }
    
}
