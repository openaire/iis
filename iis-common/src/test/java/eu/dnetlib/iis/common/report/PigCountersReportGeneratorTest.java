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
    private ReportPigCounterMappingParser reportPigCounterMappingParser;
    
    @Mock
    private ReportPigCountersResolver reportPigCountersResolver;
    
    
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
                "report.group.param1", "pigCounterName1",
                "report.group.param2", "pigCounterName2");
        
        PigCounters pigCounters = Mockito.mock(PigCounters.class);
        when(pigCountersParser.parse("counters")).thenReturn(pigCounters);
        
        ReportPigCounterMapping counterMapping1 = new ReportPigCounterMapping("group.param1", "jobAlias1", "counterName1");
        ReportPigCounterMapping counterMapping2 = new ReportPigCounterMapping("group.param2", "jobAlias2", "counterName2");
        
        when(reportPigCounterMappingParser.parse("group.param1", "pigCounterName1")).thenReturn(counterMapping1);
        when(reportPigCounterMappingParser.parse("group.param2", "pigCounterName2")).thenReturn(counterMapping2);
        
        ReportParam reportCounter1 = new ReportParam("group.param1", "2");
        ReportParam reportCounter2 = new ReportParam("group.param2" ,"8");
        
        when(reportPigCountersResolver.resolveReportCounters(Lists.newArrayList(counterMapping1, counterMapping2), pigCounters))
                .thenReturn(Lists.newArrayList(reportCounter1, reportCounter2));
        
        // execute
        
        pigCountersReportGenerator.run(portBindings, conf, parameters);
        
        // assert
        
        List<ReportParam> actualReportCounters = AvroTestUtils.readLocalAvroDataStore(tempFolder.getRoot().getPath());
        
        assertThat(actualReportCounters, containsInAnyOrder(reportCounter1, reportCounter2));
    }
    
}
