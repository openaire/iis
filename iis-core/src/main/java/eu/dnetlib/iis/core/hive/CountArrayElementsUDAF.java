package eu.dnetlib.iis.core.hive;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public final class CountArrayElementsUDAF extends UDAF {

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

        public boolean iterate(List<String> list) {
            if (list != null) {
                for (String element : list) {
                    if (element != null) {
                        if (state.countMap.get(element) == null) {
                            state.countMap.put(element, 0);
                        }
                        state.countMap.put(element, state.countMap.get(element) + 1);
                    }
                }
            }
            return true;
        }

        public UDAFState terminatePartial() {
            return state;
        }

        public void merge(UDAFState partialState) {
            if (partialState != null) {
                for (Map.Entry<String, Integer> entry : partialState.countMap.entrySet()) {
                    if (state.countMap.get(entry.getKey()) == null) {
                        state.countMap.put(entry.getKey(), 0);
                    }
                    state.countMap.put(entry.getKey(), state.countMap.get(entry.getKey()) + entry.getValue());
                }
            }
        }

        public Map<String, Integer> terminate() {
            return state.countMap;
        }

    }

    private CountArrayElementsUDAF() {
    }
}