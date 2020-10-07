package eu.dnetlib.iis.common.report;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import eu.dnetlib.iis.common.counter.PigCounters;
import eu.dnetlib.iis.common.counter.PigCountersParser;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.when;

/**
 * @author madryk
 */
@ExtendWith(MockitoExtension.class)
public class PigCountersReportGeneratorTest {

    @InjectMocks
    private PigCountersReportGenerator pigCountersReportGenerator = new PigCountersReportGenerator();
    
    @Mock
    private PigCountersParser pigCountersParser;
    
    @Mock
    private ReportPigCounterMappingParser reportPigCounterMappingParser;
    
    @Mock
    private ReportPigCountersResolver reportPigCountersResolver;

    @TempDir
    public File tempFolder;

    //------------------------ TESTS --------------------------
    
    @Test
    public void run() throws Exception {
        
        // given
        
        Path outputDirPath = new Path(tempFolder.getPath());
        PortBindings portBindings = new PortBindings(ImmutableMap.of(), ImmutableMap.of("report", outputDirPath));
        Configuration conf = new Configuration(false);
        
        Map<String, String> parameters = ImmutableMap.of(
                "pigCounters", "counters",
                "report.group.param1", "pigCounterName1",
                "report.group.param2", "pigCounterName2");
        
        PigCounters pigCounters = Mockito.mock(PigCounters.class);
        when(pigCountersParser.parse("counters")).thenReturn(pigCounters);
        
        ReportPigCounterMapping counterMapping1 = new ReportPigCounterMapping("counterName1", "jobAlias1", "group.param1");
        ReportPigCounterMapping counterMapping2 = new ReportPigCounterMapping("counterName2", "jobAlias2", "group.param2");
        
        when(reportPigCounterMappingParser.parse("group.param1", "pigCounterName1")).thenReturn(counterMapping1);
        when(reportPigCounterMappingParser.parse("group.param2", "pigCounterName2")).thenReturn(counterMapping2);
        
        ReportEntry reportCounter1 = new ReportEntry("group.param1", ReportEntryType.COUNTER, "2");
        ReportEntry reportCounter2 = new ReportEntry("group.param2", ReportEntryType.COUNTER, "8");
        
        when(reportPigCountersResolver.resolveReportCounters(pigCounters, Lists.newArrayList(counterMapping1, counterMapping2)))
                .thenReturn(Lists.newArrayList(reportCounter1, reportCounter2));
        
        // execute
        
        pigCountersReportGenerator.run(portBindings, conf, parameters);
        
        // assert
        
        List<ReportEntry> actualReportCounters = AvroTestUtils.readLocalAvroDataStore(tempFolder.getPath());
        
        assertThat(actualReportCounters, containsInAnyOrder(reportCounter1, reportCounter2));
    }
    
}
