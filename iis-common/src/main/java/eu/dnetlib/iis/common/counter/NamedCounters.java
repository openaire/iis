package eu.dnetlib.iis.common.counter;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Class that groups several counters which are identified by name (<code>String</code> value).
 * 
 * @author madryk
 */
public class NamedCounters implements Serializable {

    private static final long serialVersionUID = 1L;
    
    
    private Map<String, Long> counters;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    /**
     * Creates {@link NamedCounters} with empty initial counters.
     */
    public NamedCounters() {
        this.counters = Maps.newHashMap();
    }
    
    /**
     * Creates {@link NamedCounters} with initial counters.<br/>
     * Starting value of initial counters is zero.
     * 
     * @param initialCounterNames - names of initial counters
     */
    public NamedCounters(String[] initialCounterNames) {
        Preconditions.checkNotNull(initialCounterNames);
        
        this.counters = Maps.newHashMap();
        
        for (String initialCounterName : initialCounterNames) {
            this.counters.put(initialCounterName, 0L);
        }
    }
    
    /**
     * Creates {@link NamedCounters} with initial counters.<br/>
     * Starting value of initial counters is zero.
     * 
     * @param initialCounterNamesEnumClass - enum class providing names of initial counters
     */
    public <E extends Enum<E>> NamedCounters(Class<E> initialCounterNamesEnumClass) {
        Preconditions.checkNotNull(initialCounterNamesEnumClass);
        
        this.counters = Maps.newHashMap();
        Enum<?>[] enumConstants = initialCounterNamesEnumClass.getEnumConstants();
        
        for (int i=0; i<enumConstants.length; ++i) {
            this.counters.put(enumConstants[i].name(), 0L);
        }
        
    }
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Increments value by 1 of a counter with the name specified as parameter.<br/>
     * Internally uses {@link #increment(String, Long)} 
     */
    public void increment(String counterName) {
        increment(counterName, 1L);
    }
    
    /**
     * Increments value of a counter with the name specified as parameter by the given value.<br/>
     * If current instance of {@link NamedCounters} does not contain counter
     * with provided name, then before incrementing counter will be created with starting
     * value equal to zero.
     */
    public void increment(String counterName, Long incrementValue) {
        
        long oldValue = counters.getOrDefault(counterName, 0L);
        counters.put(counterName, oldValue + incrementValue);
    }
    
    /**
     * Returns current value of a counter with the name specified as parameter.
     * 
     * @throws IllegalArgumentException when {@link NamedCounters} does not contain counter
     *      with provided name
     */
    public long currentValue(String counterName) {
        
        if (!counters.containsKey(counterName)) {
            throw new IllegalArgumentException("Couldn't find counter with name: " + counterName);
        }
        
        return counters.get(counterName);
    }
    
    /**
     * Returns names of currently tracked counters.
     */
    public Collection<String> counterNames() {
        return counters.keySet();
    }
    
}
