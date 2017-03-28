package eu.dnetlib.iis.common.report;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.counter.NamedCounters;
import eu.dnetlib.iis.common.schemas.ReportEntry;

/**
 * Converter of {@link NamedCounters} to {@link ReportEntry} list.
 * 
 * @author madryk
 */
public class CountersToReportEntriesConverter {
    
    private final Map<String, String> counterNameToParamKeyMapping;

    
    //------------------------ CONSTRUCTORS --------------------------
    
    /**
     * Default constructor.
     * 
     * @param counterNameToEntryKeyMapping - defines mapping of counter names into report entry keys
     */
    public CountersToReportEntriesConverter(Map<String, String> counterNameToEntryKeyMapping) {
        this.counterNameToParamKeyMapping = counterNameToEntryKeyMapping;
    }
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Converts passed {@link NamedCounters} into {@link ReportEntry}.
     */
    public List<ReportEntry> convertToReportEntries(NamedCounters counters) {
        
        List<ReportEntry> reportEntries = Lists.newArrayList();
        
        for (String counterName : counters.counterNames()) {
            
            ReportEntry reportEntry = ReportEntryFactory.createCounterReportEntry(mapToReportEntryKey(counterName), counters.currentValue(counterName));
            
            reportEntries.add(reportEntry);
        }
        
        return reportEntries;
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private String mapToReportEntryKey(String counterName) {
        
        if (counterNameToParamKeyMapping.containsKey(counterName)) {
            return counterNameToParamKeyMapping.get(counterName);
        }
        return counterName;
        
    }
    
}
