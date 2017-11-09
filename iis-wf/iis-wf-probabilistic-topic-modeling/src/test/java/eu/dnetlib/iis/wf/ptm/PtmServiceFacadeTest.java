package eu.dnetlib.iis.wf.ptm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.web.client.RestTemplate;

import eu.dnetlib.ptm.service.Command;
import eu.dnetlib.ptm.service.ExecutionReport;
import eu.dnetlib.ptm.service.PtmException;

/**
 * {@link PtmServiceFacade} test class.
 * 
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class PtmServiceFacadeTest {

    private String serviceLocation = "some-domain";
    
    @Mock
    private RestTemplate restTemplate;
    
    @InjectMocks
    @Spy
    private PtmServiceFacade facade = new PtmServiceFacade(serviceLocation);
    
    @Test
    public void testAnnotateNullCommand() {
        // given
        String jobId = "someJobId";
        doReturn(jobId).when(restTemplate).getForObject(serviceLocation + '/' + "annotate", String.class);
        
        // execute
        String result = facade.annotate(null);
        
        // assert
        assertEquals(jobId, result);
    }
    
    @Test
    public void testAnnotateWithCommandParamsSet() {
        // given
        Map<String, String> map = new HashMap<>();
        map.put("paramKey", "paramValue");
        String jobId = "someJobId";
        
        doReturn(jobId).when(restTemplate).getForObject(serviceLocation + '/' + "annotate?map[paramKey]={paramKey}", String.class, map);
        
        // execute
        String result = facade.annotate(new Command(map));
        
        // assert
        assertEquals(jobId, result);
    }

    @Test
    public void testTopicModelingNullCommand() {
        // given
        String jobId = "someJobId";
        doReturn(jobId).when(restTemplate).getForObject(serviceLocation + '/' + "topic-modeling", String.class);
        
        // execute
        String result = facade.topicModeling(null);
        
        // assert
        assertEquals(jobId, result);
    }
    
    @Test
    public void testTopicModelingWithCommandParamsSet() {
        // given
        Map<String, String> map = new HashMap<>();
        map.put("paramKey", "paramValue");
        String jobId = "someJobId";
        
        doReturn(jobId).when(restTemplate).getForObject(serviceLocation + '/' + "topic-modeling?map[paramKey]={paramKey}", String.class, map);
        
        // execute
        String result = facade.topicModeling(new Command(map));
        
        // assert
        assertEquals(jobId, result);
    }
    
    @Test
    public void testGetReport() throws PtmException {
        // given
        String jobId = "someJobId";
        ExecutionReport expectedReport = new ExecutionReport();
        
        doReturn(expectedReport).when(restTemplate).getForObject(serviceLocation + '/' + "report/{jobId}", ExecutionReport.class, jobId);
        
        // execute
        ExecutionReport result = facade.getReport(jobId);
        
        // assert
        assertTrue(expectedReport == result);
    }
    
    @Test
    public void testListJobs() throws PtmException {
        // given
        Set<String> expectedJobs = new HashSet<>();
        expectedJobs.add("job1");
        expectedJobs.add("job2");
        
        doReturn(expectedJobs).when(restTemplate).getForObject(serviceLocation + '/' + "list", Set.class);
        
        // execute
        Set<String> result = facade.listJobs();
        
        // assert
        assertTrue(expectedJobs == result);
    }
    
    @Test
    public void testCancelJob() throws PtmException {
        // given
        String jobId = "someJobId";
        boolean expectedResult = true;
        
        doReturn(expectedResult).when(restTemplate).getForObject(serviceLocation + '/' + "cancel/{jobId}", Boolean.class, jobId);
        
        // execute
        boolean result = facade.cancel(jobId);
        
        // assert
        assertTrue(result);
    }
    
}
