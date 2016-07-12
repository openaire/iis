package eu.dnetlib.iis.common.report;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.counter.NamedCounters;
import eu.dnetlib.iis.common.schemas.ReportParam;

/**
 * Converter of {@link NamedCounters} to {@link ReportParam}.
 * 
 * @author madryk
 */
public class CountersToReportParamsConverter {
    
    private Map<String, String> counterNameToParamKeyMapping;

    
    //------------------------ CONSTRUCTORS --------------------------
    
    /**
     * Default constructor.
     * 
     * @param counterNameToParamKeyMapping - defines mapping of counter names into report param keys
     */
    public CountersToReportParamsConverter(Map<String, String> counterNameToParamKeyMapping) {
        this.counterNameToParamKeyMapping = counterNameToParamKeyMapping;
    }
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Converts passed {@link NamedCounters} into {@link ReportParam}.
     */
    public List<ReportParam> convertToReportParams(NamedCounters counters) {
        
        List<ReportParam> reportParams = Lists.newArrayList();
        
        for (String counterName : counters.counterNames()) {
            
            ReportParam reportParam = new ReportParam(mapToReportParamKey(counterName), String.valueOf(counters.currentValue(counterName)));
            
            reportParams.add(reportParam);
        }
        
        return reportParams;
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private String mapToReportParamKey(String counterName) {
        
        if (counterNameToParamKeyMapping.containsKey(counterName)) {
            return counterNameToParamKeyMapping.get(counterName);
        }
        return counterName;
        
    }
    
}
