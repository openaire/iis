package eu.dnetlib.iis.common.report;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.counter.PigCounters;
import eu.dnetlib.iis.common.counter.PigCountersParser;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.schemas.ReportParam;
import eu.dnetlib.iis.common.utils.AvroTestUtils;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class PigCountersReportGeneratorTest {

    @InjectMocks
    private PigCountersReportGenerator pigCountersReportGenerator = new PigCountersReportGenerator();
    
    @Mock
    private PigCountersParser pigCountersParser;
    
    @Mock
    private PigCounterValueResolver pigCounterValueResolver;
    
    
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void run() throws Exception {
        
        // given
        
        Path outputDirPath = new Path(tempFolder.getRoot().getPath());
        PortBindings portBindings = new PortBindings(ImmutableMap.of(), ImmutableMap.of("report", outputDirPath));
        Configuration conf = new Configuration(false);
        
        Map<String, String> parameters = ImmutableMap.of(
                "pigCounters", "counters",
                "report.group.param1", "paramValue1",
                "report.group.param2", "paramValue2");
        
        PigCounters pigCounters = Mockito.mock(PigCounters.class);
        
        when(pigCountersParser.parse("counters")).thenReturn(pigCounters);
        when(pigCounterValueResolver.resolveValue("paramValue1", pigCounters)).thenReturn("2");
        when(pigCounterValueResolver.resolveValue("paramValue2", pigCounters)).thenReturn("8");
        
        // execute
        
        pigCountersReportGenerator.run(portBindings, conf, parameters);
        
        // assert
        
        List<ReportParam> actualReportParams = AvroTestUtils.readLocalAvroDataStore(tempFolder.getRoot().getPath());
        
        assertThat(actualReportParams, containsInAnyOrder(
                new ReportParam("group.param1", "2"),
                new ReportParam("group.param2", "8")));
    }
    
}
