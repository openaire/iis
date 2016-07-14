package eu.dnetlib.iis.common.counter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

/**
 * @author madryk
 */
public class NamedCountersAccumulableParamTest {

    private NamedCountersAccumulableParam countersAccumulableParam = new NamedCountersAccumulableParam();
    
    
    private String counterName1 = "COUNTER_1";
    
    private String counterName2 = "COUNTER_2";
    
    private String counterName3 = "COUNTER_3";
    
    
    private NamedCounters namedCounters1 = new NamedCounters(new String[] {counterName1, counterName2});
    
    private NamedCounters namedCounters2 = new NamedCounters(new String[] {counterName2, counterName3});
    
    
    @Before
    public void setup() {
        namedCounters1.increment(counterName1, 3L);
        namedCounters1.increment(counterName2, 1L);
        namedCounters2.increment(counterName2, 7L);
        namedCounters2.increment(counterName3, 5L);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void addAccumulator() {
        
        // execute
        
        NamedCounters retNamedCounters = countersAccumulableParam.addAccumulator(namedCounters1, new Tuple2<>(counterName2, 9L));
        
        // assert
        
        assertTrue(retNamedCounters == namedCounters1);
        
        assertThat(retNamedCounters.counterNames(), containsInAnyOrder(counterName1, counterName2));
        assertThat(retNamedCounters.currentValue(counterName1), is(3L));
        assertThat(retNamedCounters.currentValue(counterName2), is(10L));
    }
    
    @Test
    public void addInPlace() {
        
        // execute
        
        NamedCounters retNamedCounters = countersAccumulableParam.addInPlace(namedCounters1, namedCounters2);
        
        // assert
        
        assertTrue(retNamedCounters == namedCounters1);
        
        assertThat(retNamedCounters.counterNames(), containsInAnyOrder(counterName1, counterName2, counterName3));
        assertThat(retNamedCounters.currentValue(counterName1), is(3L));
        assertThat(retNamedCounters.currentValue(counterName2), is(8L));
        assertThat(retNamedCounters.currentValue(counterName3), is(5L));
    }
    
    @Test
    public void zero() {
        
        // execute
        
        NamedCounters retNamedCounters = countersAccumulableParam.zero(namedCounters1);
        
        // assert
        
        assertTrue(retNamedCounters == namedCounters1);
        
        assertThat(retNamedCounters.counterNames(), containsInAnyOrder(counterName1, counterName2));
        assertThat(retNamedCounters.currentValue(counterName1), is(3L));
        assertThat(retNamedCounters.currentValue(counterName2), is(1L));
    }
    
}
