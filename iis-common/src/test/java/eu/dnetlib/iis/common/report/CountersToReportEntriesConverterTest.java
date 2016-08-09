package eu.dnetlib.iis.common.report;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import eu.dnetlib.iis.common.counter.NamedCounters;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;

/**
 * @author madryk
 */
public class CountersToReportEntriesConverterTest {

    private CountersToReportEntriesConverter converter;
    
    
    private String counterName1 = "COUNTER_1";
    
    private String counterName2 = "COUNTER_2";
    
    private String counterName3 = "COUNTER_3";
    
    
    private String reportEntryKey1 = "report.param.count.1";
    
    private String reportEntryKey2 = "report.param.count.2";
    
    
    
    @Before
    public void setup() {
        
        Map<String, String> counterNameToEntryKeyMapping = ImmutableMap.of(
                counterName1, reportEntryKey1,
                counterName2, reportEntryKey2);
        
        
        converter = new CountersToReportEntriesConverter(counterNameToEntryKeyMapping);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void convert() {
        
        // given
        
        NamedCounters namedCounters = new NamedCounters(new String[] {counterName1, counterName2, counterName3});
        namedCounters.increment(counterName1, 3L);
        namedCounters.increment(counterName2, 4L);
        namedCounters.increment(counterName3, 5L);
        
        // execute
        
        List<ReportEntry> reportEntries = converter.convertToReportEntries(namedCounters);
        
        // assert
        
        assertThat(reportEntries, containsInAnyOrder(
                new ReportEntry(reportEntryKey1, ReportEntryType.COUNTER, "3"),
                new ReportEntry(reportEntryKey2, ReportEntryType.COUNTER, "4"),
                new ReportEntry(counterName3, ReportEntryType.COUNTER, "5")));
    }
    
}
