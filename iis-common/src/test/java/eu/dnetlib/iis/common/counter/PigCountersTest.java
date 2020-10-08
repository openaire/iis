package eu.dnetlib.iis.common.counter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import eu.dnetlib.iis.common.counter.PigCounters.JobCounters;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author madryk
 */
@ExtendWith(MockitoExtension.class)
public class PigCountersTest {

    private PigCounters pigCounters;
    
    
    private JobCounters jobCounters1;
    
    private JobCounters jobCounters2;
    
    @Mock
    private Map<String, String> rootLevelCounters;
    
    
    @BeforeEach
    public void setup() {
        
        jobCounters1 = new JobCounters("JOB_ID_1");
        jobCounters1.addAlias("JOB_1_ALIAS");
        jobCounters1.addAlias("JOB_1_ALIAS_2");
        
        jobCounters1.addCounter("COUNTER_1", "34");
        
        jobCounters2 = new JobCounters("JOB_ID_2");
        jobCounters2.addCounter("COUNTER_1", "13");
        jobCounters2.addCounter("COUNTER_2", "vv");
        
        
        
        pigCounters = new PigCounters(rootLevelCounters, Lists.newArrayList(jobCounters1, jobCounters2));
        
    }
    
    @Test
    public void getJobIds() {
        // execute & assert
        assertThat(pigCounters.getJobIds(), containsInAnyOrder("JOB_ID_1", "JOB_ID_2"));
    }
    
    @Test
    public void getJobCounters() {
        // execute
        JobCounters jobCounters = pigCounters.getJobCounters("JOB_ID_2");
        // assert
        assertSame(jobCounters, jobCounters2);
    }
    
    @Test
    public void getJobCounters_JOB_ID_NOT_FOUND() {
        // execute & assert
        assertNull(pigCounters.getJobCounters("INVALID_JOB_ID"));
    }
    
    @Test
    public void getJobIdByAlias() {
        // execute
        String jobId = pigCounters.getJobIdByAlias("JOB_1_ALIAS_2");
        // assert
        assertEquals(jobCounters1.getJobId(), jobId);
    }
    
    @Test
    public void getJobIdByAlias_JOB_ALIAS_NOT_FOUND() {
        // execute & assert
        assertNull(pigCounters.getJobIdByAlias("INVALID_ALIAS"));
    }

    @Test
    public void getRootLevelCounters() {
        
        // execute & assert
        assertSame(rootLevelCounters, pigCounters.getRootLevelCounters());
    }
    
    @Test
    public void constuctor_rootLevelCounters_NULL() {
        
        // execute
        assertThrows(NullPointerException.class, () -> new PigCounters(null, ImmutableList.of()));
    }
    
    @Test
    public void constuctor_jobLevelCounters_NULL() {
        
        // execute
        assertThrows(NullPointerException.class, () -> new PigCounters(rootLevelCounters, null));
    }
}
