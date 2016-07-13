package eu.dnetlib.iis.common.counter;

import org.apache.spark.AccumulableParam;

import scala.Tuple2;

/**
 * Spark {@link AccumulableParam} for tracking multiple counter values using {@link NamedCounters}.
 * 
 * @author madryk
 */
public class NamedCountersAccumulableParam implements AccumulableParam<NamedCounters, Tuple2<String,Long>> {

    private static final long serialVersionUID = 1L;

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Increments {@link NamedCounters} counter with the name same as the first element of passed incrementValue tuple
     * by value defined in the second element of incrementValue tuple.
     */
    @Override
    public NamedCounters addAccumulator(NamedCounters counters, Tuple2<String, Long> incrementValue) {
        counters.increment(incrementValue._1, incrementValue._2);
        return counters;
    }

    /**
     * Merges two passed {@link NamedCounters}.
     */
    @Override
    public NamedCounters addInPlace(NamedCounters counters1, NamedCounters counters2) {
        for (String counterName2 : counters2.counterNames()) {
            counters1.increment(counterName2, counters2.currentValue(counterName2));
        }
        return counters1;
    }

    /**
     * Returns passed initialCounters value without any modifications.
     */
    @Override
    public NamedCounters zero(NamedCounters initialCounters) {
        return initialCounters;
    }

}
