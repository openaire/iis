package eu.dnetlib.iis.common.report;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import eu.dnetlib.iis.common.counter.PigCounters;
import eu.dnetlib.iis.common.counter.PigCounters.JobCounters;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;

/**
 * @author madryk
 */
public class ReportPigCountersResolverTest {
    
    private ReportPigCountersResolver reportPigCountersResolver = new ReportPigCountersResolver();
    
    private PigCounters pigCounters;
    
    @Before
    public void setup() {

        JobCounters jobCounters1 = new JobCounters("job_ID1");
        jobCounters1.addAlias("jobAlias1");
        jobCounters1.addAlias("jobAlias1_2");

        jobCounters1.addCounter("MAP_INPUT_RECORDS", "10");
        jobCounters1.addCounter("MAP_OUTPUT_RECORDS", "9");
        jobCounters1.addCounter("REDUCE_INPUT_RECORDS", "9");
        jobCounters1.addCounter("REDUCE_OUTPUT_RECORDS", "2");


        JobCounters jobCounters2 = new JobCounters("job_ID2");
        jobCounters2.addAlias("jobAlias2");

        jobCounters2.addCounter("MAP_INPUT_RECORDS", "3");
        
        Map<String, String> rootLevelCounters = Maps.newHashMap();
        rootLevelCounters.put("RECORD_WRITTEN", "3");
        rootLevelCounters.put("SOME_STRANGE_COUNTER", "500");

        pigCounters = new PigCounters(rootLevelCounters, Lists.newArrayList(jobCounters1, jobCounters2));

    }
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = IllegalArgumentException.class)
    public void resolveReportCounters_jobLevelCounter_NON_EXISTENT_JOB_ALIAS() {
        
        // given
        
        ReportPigCounterMapping counterMapping = new ReportPigCounterMapping("MAP_INPUT_RECORDS", "nonexistentJobAlias", "destination.report.param1");
        
        // execute
        
        reportPigCountersResolver.resolveReportCounters(pigCounters, Lists.newArrayList(counterMapping));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void resolveReportCounters_jobLevelCounter_NON_EXISTENT_COUNTER_NAME() {
        
        // given
        
        ReportPigCounterMapping counterMapping = new ReportPigCounterMapping("NON_EXISTENT_COUNTER_NAME", "jobAlias1", "destination.report.param1");
        
        // execute
        
        reportPigCountersResolver.resolveReportCounters(pigCounters, Lists.newArrayList(counterMapping));
    }

    @Test(expected = IllegalArgumentException.class)
    public void resolveReportCounters_rootLevelCounter_NON_EXISTENT_COUNTER_NAME() {
        
        // given
        
        ReportPigCounterMapping counterMapping = new ReportPigCounterMapping("NON_EXISTENT_COUNTER_NAME", null, "destination.report.param1");
        
        // execute
        
        reportPigCountersResolver.resolveReportCounters(pigCounters, Lists.newArrayList(counterMapping));
    }

    
    @Test
    public void resolveReportCounters() {
        
        // given
        
        ReportPigCounterMapping counterMapping1 = new ReportPigCounterMapping("MAP_INPUT_RECORDS", "jobAlias1_2", "destination.report.param1");
        ReportPigCounterMapping counterMapping2 = new ReportPigCounterMapping("REDUCE_OUTPUT_RECORDS", "jobAlias1", "destination.report.param2");
        ReportPigCounterMapping counterMapping3 = new ReportPigCounterMapping("MAP_INPUT_RECORDS", "jobAlias2", "destination.report.param3");
        ReportPigCounterMapping counterMapping4 = new ReportPigCounterMapping("RECORD_WRITTEN", null, "destination.report.record_written");
        ReportPigCounterMapping counterMapping5 = new ReportPigCounterMapping("SOME_STRANGE_COUNTER", null, "destination.report.some_strange");
        
        
        // execute
        
        List<ReportEntry> reportCounters = reportPigCountersResolver.resolveReportCounters(pigCounters, Lists.newArrayList(counterMapping1, counterMapping2, counterMapping3, counterMapping4, counterMapping5));
        
        // assert
        
        assertThat(reportCounters, containsInAnyOrder(
                new ReportEntry("destination.report.param1", ReportEntryType.COUNTER, "10"),
                new ReportEntry("destination.report.param2", ReportEntryType.COUNTER, "2"),
                new ReportEntry("destination.report.param3", ReportEntryType.COUNTER, "3"),
                new ReportEntry("destination.report.record_written", ReportEntryType.COUNTER, "3"),
                new ReportEntry("destination.report.some_strange", ReportEntryType.COUNTER, "500")));

    }
    
    
}
