package eu.dnetlib.iis.common.counter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

/**
 * @author madryk
 */
public class NamedCountersTest {

    private String counterName1 = "COUNTER_1";
    
    private String counterName2 = "COUNTER_2";
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void constructor_EMPTY_INITIAL() {
        
        // execute
        
        NamedCounters namedCounters = new NamedCounters();
        
        // assert
        
        assertThat(namedCounters.counterNames(), is(empty()));
        
    }
    
    @Test
    public void constructor_INITIAL_COUNTERS() {
        
        // execute
        
        NamedCounters namedCounters = new NamedCounters(new String[] {counterName1, counterName2});
        
        // assert
        
        assertThat(namedCounters.counterNames(), containsInAnyOrder(counterName1, counterName2));
        assertThat(namedCounters.currentValue(counterName1), is(0L));
        assertThat(namedCounters.currentValue(counterName2), is(0L));
    }
    
    @Test
    public void constructor_INITIAL_COUNTERS_ENUM_NAMES() {
        
        // execute
        
        NamedCounters namedCounters = new NamedCounters(CountersGroup.class);
        
        // assert
        
        assertThat(namedCounters.counterNames(), containsInAnyOrder(CountersGroup.ENUM_COUNTER_1.name(), CountersGroup.ENUM_COUNTER_2.name()));
        assertThat(namedCounters.currentValue(CountersGroup.ENUM_COUNTER_1.name()), is(0L));
        assertThat(namedCounters.currentValue(CountersGroup.ENUM_COUNTER_2.name()), is(0L));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void currentValue_NOT_VALID_COUNTER_NAME() {
        
        // given
        
        NamedCounters namedCounters = new NamedCounters(new String[] {counterName1, counterName2});
        
        // execute
        
        namedCounters.currentValue("NOT_EXISTING_COUNTER");
    }
    
    @Test
    public void increment_EXISTING_COUNTER() {
        
        // given
        
        NamedCounters namedCounters = new NamedCounters(new String[] {counterName1});
        
        
        // execute
        
        namedCounters.increment(counterName1);
        namedCounters.increment(counterName1);
        
        // assert
        
        assertThat(namedCounters.counterNames(), containsInAnyOrder(counterName1));
        assertThat(namedCounters.currentValue(counterName1), is(2L));
    }
    
    @Test
    public void increment_NOT_EXISTING_COUNTER() {
        
        // given
        
        NamedCounters namedCounters = new NamedCounters(new String[] {counterName1});
        
        // execute
        
        namedCounters.increment(counterName2);
        namedCounters.increment(counterName2);
        
        // assert
        
        assertThat(namedCounters.counterNames(), containsInAnyOrder(counterName1, counterName2));
        assertThat(namedCounters.currentValue(counterName1), is(0L));
        assertThat(namedCounters.currentValue(counterName2), is(2L));
    }
    
    @Test
    public void increment_WITH_INCREMENT_VALUE_EXISTING_COUNTER() {
        
        // given
        
        NamedCounters namedCounters = new NamedCounters(new String[] {counterName1});
        
        
        // execute
        
        namedCounters.increment(counterName1, 5L);
        namedCounters.increment(counterName1, 7L);
        
        // assert
        
        assertThat(namedCounters.counterNames(), containsInAnyOrder(counterName1));
        assertThat(namedCounters.currentValue(counterName1), is(12L));
    }
    
    @Test
    public void increment_WITH_INCREMENT_VALUE_NOT_EXISTING_COUNTER() {
        
        // given
        
        NamedCounters namedCounters = new NamedCounters(new String[] {counterName1});
        
        
        // execute
        
        namedCounters.increment(counterName2, 5L);
        namedCounters.increment(counterName2, 7L);
        
        // assert
        
        assertThat(namedCounters.counterNames(), containsInAnyOrder(counterName1, counterName2));
        assertThat(namedCounters.currentValue(counterName1), is(0L));
        assertThat(namedCounters.currentValue(counterName2), is(12L));
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private static enum CountersGroup { ENUM_COUNTER_1, ENUM_COUNTER_2 }
    
}
