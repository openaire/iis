package eu.dnetlib.iis.common.report;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.when;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.oozie.OozieClientFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import eu.dnetlib.iis.common.utils.AvroTestUtils;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class OozieTimeReportGeneratorTest {

    @InjectMocks
    private OozieTimeReportGenerator oozieTimeReportGenerator = new OozieTimeReportGenerator();
    
    @Mock
    private OozieClientFactory oozieClientFactory;
    
    @Mock
    private OozieClient oozieClient;
    
    
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    
    
    private String oozieUrl = "http://oozieLocation.com:11000/oozie/";
    
    private String jobId = "WORKFLOW_JOB_ID";
    
    
    @Mock
    private WorkflowJob workflowJob;
    
    @Mock
    private WorkflowAction workflowAction1;
    
    @Mock
    private WorkflowAction workflowAction2;
    
    @Mock
    private WorkflowAction workflowAction3;
    
    
    @Before
    public void setup() throws OozieClientException, ParseException {
        
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        
        when(workflowAction1.getName()).thenReturn("action_1");
        when(workflowAction1.getStartTime()).thenReturn(dateFormat.parse("2016-08-04 05:17:56"));
        when(workflowAction1.getEndTime()).thenReturn(dateFormat.parse("2016-08-04 05:18:10")); // duration: 14sec
        
        when(workflowAction2.getName()).thenReturn("action_2");
        when(workflowAction2.getStartTime()).thenReturn(dateFormat.parse("2016-08-04 05:18:10"));
        when(workflowAction2.getEndTime()).thenReturn(dateFormat.parse("2016-08-04 05:19:13")); // duration: 1min 3sec
        
        when(workflowAction3.getName()).thenReturn("action_3");
        when(workflowAction3.getStartTime()).thenReturn(dateFormat.parse("2016-08-04 05:18:10"));
        when(workflowAction3.getEndTime()).thenReturn(dateFormat.parse("2016-08-04 06:21:42")); // duration: 1h 3min 32sec
        
        when(workflowJob.getActions()).thenReturn(Lists.newArrayList(workflowAction1, workflowAction2, workflowAction3));
        
        
        when(oozieClient.getJobInfo(jobId)).thenReturn(workflowJob);
    }
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test
    public void run() throws Exception {
        
        // given
        
        PortBindings portBindings = createPortBindings();
        
        Configuration conf = new Configuration(false);
        
        Map<String, String> parameters = ImmutableMap.of(
                "oozieServiceLoc", oozieUrl,
                "jobId", jobId,
                "report.group.first", "action_1,action_3,nonexisting_action_1",
                "report.group.second", "action_2",
                "report.group.nonexisting", "nonexisting_action_1, nonexisting_action_2");
        
        
        when(oozieClientFactory.createOozieClient(oozieUrl)).thenReturn(oozieClient);
        
        
        // execute
        
        oozieTimeReportGenerator.run(portBindings, conf, parameters);
        
        
        // assert
        
        List<ReportEntry> actualReport = AvroTestUtils.readLocalAvroDataStore(tempFolder.getRoot().getPath());
        
        assertThat(actualReport, containsInAnyOrder(
                new ReportEntry("group.first", ReportEntryType.DURATION, "3826000"), // 1h 3min 46sec
                new ReportEntry("group.second", ReportEntryType.DURATION, "63000"))); // 1min 3sec
    }

    
    //------------------------ PRIVATE --------------------------

    private PortBindings createPortBindings() {
        Path outputDirPath = new Path(tempFolder.getRoot().getPath());
        PortBindings portBindings = new PortBindings(ImmutableMap.of(), ImmutableMap.of("report", outputDirPath));
        return portBindings;
    }
    
}
