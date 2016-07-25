package eu.dnetlib.iis.common.report;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.counter.PigCounters;
import eu.dnetlib.iis.common.counter.PigCounters.JobCounters;
import eu.dnetlib.iis.common.schemas.ReportParam;

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

        pigCounters = new PigCounters(Lists.newArrayList(jobCounters1, jobCounters2));

    }
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = IllegalArgumentException.class)
    public void resolveReportCounters_INVALID_ALIAS() {
        
        // given
        
        ReportPigCounterMapping counterMapping = new ReportPigCounterMapping("destination.report.param1", "invalidJobAlias", "MAP_INPUT_RECORDS");
        
        // execute
        
        reportPigCountersResolver.resolveReportCounters(Lists.newArrayList(counterMapping), pigCounters);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void resolveReportCounters_INVALID_COUNTER_NAME() {
        
        // given
        
        ReportPigCounterMapping counterMapping = new ReportPigCounterMapping("destination.report.param1", "jobAlias1", "INVALID_COUNTER_NAME");
        
        // execute
        
        reportPigCountersResolver.resolveReportCounters(Lists.newArrayList(counterMapping), pigCounters);
    }
    
    @Test
    public void resolveReportCounters() {
        
        // given
        
        ReportPigCounterMapping counterMapping1 = new ReportPigCounterMapping("destination.report.param1", "jobAlias1_2", "MAP_INPUT_RECORDS");
        ReportPigCounterMapping counterMapping2 = new ReportPigCounterMapping("destination.report.param2", "jobAlias1", "REDUCE_OUTPUT_RECORDS");
        ReportPigCounterMapping counterMapping3 = new ReportPigCounterMapping("destination.report.param3", "jobAlias2", "MAP_INPUT_RECORDS");
        
        // execute
        
        List<ReportParam> reportCounters = reportPigCountersResolver.resolveReportCounters(Lists.newArrayList(counterMapping1, counterMapping2, counterMapping3), pigCounters);
        
        // assert
        
        assertThat(reportCounters, containsInAnyOrder(
                new ReportParam("destination.report.param1", "10"),
                new ReportParam("destination.report.param2", "2"),
                new ReportParam("destination.report.param3", "3")));
    }
}
