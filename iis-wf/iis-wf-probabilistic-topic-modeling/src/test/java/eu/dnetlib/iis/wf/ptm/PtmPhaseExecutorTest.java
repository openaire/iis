package eu.dnetlib.iis.wf.ptm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.ptm.service.Command;
import eu.dnetlib.ptm.service.ErrorDetails;
import eu.dnetlib.ptm.service.ExecutionReport;
import eu.dnetlib.ptm.service.JobStatus;
import eu.dnetlib.ptm.service.PtmService;

/**
 * {@link PtmPhaseExecutor} test class.
 * 
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class PtmPhaseExecutorTest {
    
    
    private PortBindings portBindings = null;
    
    private Configuration conf = null;
    
    private Map<String, String> parameters;
    
    @Mock
    private PtmService mockedPtmService;

    @Captor
    ArgumentCaptor<Command> commandCaptor;
    
    private PtmPhaseExecutor process = new PtmPhaseExecutor() {
        
        protected PtmService buildServiceFacade(String serviceLocation) {
            return mockedPtmService;
        };
    };
    
    @Before
    public void init() {
        parameters = new HashMap<>();
    }

    // ------------------------------------- TESTS ----------------------------------------------
    
    @Test
    public void testGetInputPorts() {
        // execute & assert
        assertNotNull(process.getInputPorts());
        assertEquals(0, process.getInputPorts().size());
    }
    
    @Test
    public void testGetOutputPorts() {
        // execute & assert
        assertNotNull(process.getOutputPorts());
        assertEquals(0, process.getOutputPorts().size());
    }
    
    @Test(expected=RuntimeException.class)
    public void testRunMissingPhase() throws Exception {
        // given
        parameters.put(PtmPhaseExecutor.SERVICE_LOCATION_PARAM, "irrelevant");
        // execute
        process.run(portBindings, conf, parameters);
    }
    
    @Test(expected=RuntimeException.class)
    public void testRunMissingServiceLocation() throws Exception {
        // given
        parameters.put(PtmPhaseExecutor.PHASE_PARAM, PtmPhaseExecutor.PHASE_NAME_ANNOTATE);
        // execute
        process.run(portBindings, conf, parameters);
    }
    
    @Test(expected=RuntimeException.class)
    public void testRunInvalidPhase() throws Exception {
        // given
        parameters.put(PtmPhaseExecutor.SERVICE_LOCATION_PARAM, "irrelevant");
        parameters.put(PtmPhaseExecutor.PHASE_PARAM, "unsupported");
        // execute
        process.run(portBindings, conf, parameters);
    }
    
    @Test
    public void testRunTopicModelingPhaseSucceeded() throws Exception {
        // given
        parameters.put(PtmPhaseExecutor.SERVICE_LOCATION_PARAM, "irrelevant");
        parameters.put(PtmPhaseExecutor.PHASE_PARAM, PtmPhaseExecutor.PHASE_NAME_TOPIC_MODELING);
        
        String ptmParamKey = "some.ptm.param.key";
        String ptmParamValue = "some.ptm.param.value";
        parameters.put(PtmPhaseExecutor.PTM_PARAM_PREFIX + ptmParamKey, ptmParamValue);
        String jobId = "ptm-job-id";
        doReturn(jobId).when(mockedPtmService).topicModeling(any());

        when(mockedPtmService.getReport(jobId)).thenReturn(
                new ExecutionReport(null, JobStatus.succeeded, null));
        
        // execute
        process.run(portBindings, conf, parameters);
        // assert
        
        verify(mockedPtmService).topicModeling(commandCaptor.capture());
        Command capturedCommand = commandCaptor.getValue();
        assertEquals(1, capturedCommand.getMap().size());
        assertEquals(ptmParamValue, capturedCommand.getMap().get(ptmParamKey));
    }
    
    @Test
    public void testRunExportPhaseSucceeded() throws Exception {
        // given
        parameters.put(PtmPhaseExecutor.SERVICE_LOCATION_PARAM, "irrelevant");
        parameters.put(PtmPhaseExecutor.PHASE_PARAM, PtmPhaseExecutor.PHASE_NAME_EXPORT);
        
        String ptmParamKey = "some.ptm.param.key";
        String ptmParamValue = "some.ptm.param.value";
        parameters.put(PtmPhaseExecutor.PTM_PARAM_PREFIX + ptmParamKey, ptmParamValue);
        String jobId = "ptm-job-id";
        doReturn(jobId).when(mockedPtmService).export(any());

        when(mockedPtmService.getReport(jobId)).thenReturn(
                new ExecutionReport(null, JobStatus.succeeded, null));
        
        // execute
        process.run(portBindings, conf, parameters);
        // assert
        
        verify(mockedPtmService).export(commandCaptor.capture());
        Command capturedCommand = commandCaptor.getValue();
        assertEquals(1, capturedCommand.getMap().size());
        assertEquals(ptmParamValue, capturedCommand.getMap().get(ptmParamKey));
    }
    
    @Test
    public void testRunAnnotatePhaseSucceeded() throws Exception {
        // given
        parameters.put(PtmPhaseExecutor.SERVICE_LOCATION_PARAM, "irrelevant");
        parameters.put(PtmPhaseExecutor.PHASE_PARAM, PtmPhaseExecutor.PHASE_NAME_ANNOTATE);
        
        String ptmParamKey = "some.ptm.param.key";
        String ptmParamValue = "some.ptm.param.value";
        parameters.put(PtmPhaseExecutor.PTM_PARAM_PREFIX + ptmParamKey, ptmParamValue);
        String jobId = "ptm-job-id";
        doReturn(jobId).when(mockedPtmService).annotate(any());

        when(mockedPtmService.getReport(jobId)).thenReturn(
                new ExecutionReport(null, JobStatus.succeeded, null));
        
        // execute
        process.run(portBindings, conf, parameters);
        // assert
        
        verify(mockedPtmService).annotate(commandCaptor.capture());
        Command capturedCommand = commandCaptor.getValue();
        assertEquals(1, capturedCommand.getMap().size());
        assertEquals(ptmParamValue, capturedCommand.getMap().get(ptmParamKey));
    }
    
    @Test
    public void testRunAnnotatePhaseOngoingAndSucceeded() throws Exception {
        // given
        parameters.put(PtmPhaseExecutor.SERVICE_LOCATION_PARAM, "irrelevant");
        parameters.put(PtmPhaseExecutor.PHASE_PARAM, PtmPhaseExecutor.PHASE_NAME_ANNOTATE);
        parameters.put(PtmPhaseExecutor.CHECK_STATUS_INTERVAL_SECS_PARAM, "1");
        
        String ptmParamKey = "some.ptm.param.key";
        String ptmParamValue = "some.ptm.param.value";
        parameters.put(PtmPhaseExecutor.PTM_PARAM_PREFIX + ptmParamKey, ptmParamValue);
        String jobId = "ptm-job-id";
        doReturn(jobId).when(mockedPtmService).annotate(any());

        when(mockedPtmService.getReport(jobId)).thenReturn(
                new ExecutionReport(null, JobStatus.ongoing, null), 
                new ExecutionReport(null, JobStatus.succeeded, null));
        
        // execute
        process.run(portBindings, conf, parameters);
        // assert
        
        verify(mockedPtmService).annotate(commandCaptor.capture());
        Command capturedCommand = commandCaptor.getValue();
        assertEquals(1, capturedCommand.getMap().size());
        assertEquals(ptmParamValue, capturedCommand.getMap().get(ptmParamKey));
    }
    
    @Test(expected=RuntimeException.class)
    public void testRunAnnotatePhaseFailed() throws Exception {
        // given
        parameters.put(PtmPhaseExecutor.SERVICE_LOCATION_PARAM, "irrelevant");
        parameters.put(PtmPhaseExecutor.PHASE_PARAM, PtmPhaseExecutor.PHASE_NAME_ANNOTATE);
        String jobId = "ptm-job-id";
        doReturn(jobId).when(mockedPtmService).annotate(any());

        when(mockedPtmService.getReport(jobId)).thenReturn(
                new ExecutionReport(null, JobStatus.failed, new ErrorDetails()));
        
        // execute
        process.run(portBindings, conf, parameters);
    }
    
    @Test(expected=RuntimeException.class)
    public void testRunAnnotatePhaseInterrupted() throws Exception {
        // given
        parameters.put(PtmPhaseExecutor.SERVICE_LOCATION_PARAM, "irrelevant");
        parameters.put(PtmPhaseExecutor.PHASE_PARAM, PtmPhaseExecutor.PHASE_NAME_ANNOTATE);
        String jobId = "ptm-job-id";
        doReturn(jobId).when(mockedPtmService).annotate(any());

        when(mockedPtmService.getReport(jobId)).thenReturn(
                new ExecutionReport(null, JobStatus.interrupted, null));
        
        // execute
        process.run(portBindings, conf, parameters);
    }
    
}
