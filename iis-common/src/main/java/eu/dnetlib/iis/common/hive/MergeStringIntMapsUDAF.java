package eu.dnetlib.iis.common.hive;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public final class MergeStringIntMapsUDAF extends UDAF {

    public static class UDAFState {

        private Map<String, Integer> countMap;
    }

    public static class UDAFExampleAvgEvaluator implements UDAFEvaluator {

        UDAFState state;

        public UDAFExampleAvgEvaluator() {
            super();
            state = new UDAFState();
            init();
        }

        @Override
        public void init() {
            state.countMap = new HashMap<String, Integer>();
        }

        public boolean iterate(Map<String, Integer> map) {
            if (map != null) {
                for (Map.Entry<String, Integer> entry : map.entrySet()) {
                    if (state.countMap.get(entry.getKey()) == null) {
                        state.countMap.put(entry.getKey(), 0);
                    }
                    state.countMap.put(entry.getKey(), state.countMap.get(entry.getKey()) + entry.getValue());
                }
            }
            return true;
        }

        public UDAFState terminatePartial() {
            return state;
        }

        public boolean merge(UDAFState partialState) {
            if (partialState != null) {
                for (Map.Entry<String, Integer> entry : partialState.countMap.entrySet()) {
                    if (state.countMap.get(entry.getKey()) == null) {
                        state.countMap.put(entry.getKey(), 0);
                    }
                    state.countMap.put(entry.getKey(), state.countMap.get(entry.getKey()) + entry.getValue());
                }
            }
            return true;
        }

        public Map<String, Integer> terminate() {
            return state.countMap;
        }
    }

    private MergeStringIntMapsUDAF() {
    }
}