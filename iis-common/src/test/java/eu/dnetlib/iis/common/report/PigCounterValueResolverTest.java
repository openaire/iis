package eu.dnetlib.iis.common.report;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.counter.PigCounters;
import eu.dnetlib.iis.common.counter.PigCounters.JobCounters;

/**
 * @author madryk
 */
public class PigCounterValueResolverTest {

    private PigCounterValueResolver pigCounterValueResolver = new PigCounterValueResolver();
    
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
    public void resolveValue_INVALID_PLACEHOLDER() {
        
        // execute
        pigCounterValueResolver.resolveValue("{INVALID_PLACEHOLDER}", pigCounters);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void resolveValue_INVALID_JOB_ALIAS() {
        
        // execute
        pigCounterValueResolver.resolveValue("{jobAlias1_3.MAP_INPUT_RECORDS}", pigCounters);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void resolveValue_INVALID_COUNTER_NAME() {
        
        // execute
        pigCounterValueResolver.resolveValue("{jobAlias1.INVALID}", pigCounters);
    }
    
    
    @Test
    public void resolveValue_NO_PLACEHOLDER() {
        
        // execute & assert
        assertEquals("some_value", pigCounterValueResolver.resolveValue("some_value", pigCounters));
        
    }
    
    @Test
    public void resolveValue_WITH_PLACEHOLDER() {
        
        // execute & assert
        assertEquals("10", pigCounterValueResolver.resolveValue("{jobAlias1_2.MAP_INPUT_RECORDS}", pigCounters));
        
    }
    
}
