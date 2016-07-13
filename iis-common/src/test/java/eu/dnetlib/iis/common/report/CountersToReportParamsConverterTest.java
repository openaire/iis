package eu.dnetlib.iis.common.report;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import eu.dnetlib.iis.common.counter.NamedCounters;
import eu.dnetlib.iis.common.schemas.ReportParam;

/**
 * @author madryk
 */
public class CountersToReportParamsConverterTest {

    private CountersToReportParamsConverter converter;
    
    
    private String counterName1 = "COUNTER_1";
    
    private String counterName2 = "COUNTER_2";
    
    private String counterName3 = "COUNTER_3";
    
    
    private String reportParamKey1 = "report.param.count.1";
    
    private String reportParamKey2 = "report.param.count.2";
    
    
    
    @Before
    public void setup() {
        
        Map<String, String> counterNameToParamKeyMapping = ImmutableMap.of(
                counterName1, reportParamKey1,
                counterName2, reportParamKey2);
        
        
        converter = new CountersToReportParamsConverter(counterNameToParamKeyMapping);
        
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
        
        List<ReportParam> reportParams = converter.convertToReportParams(namedCounters);
        
        // assert
        
        assertThat(reportParams, containsInAnyOrder(
                new ReportParam(reportParamKey1, "3"),
                new ReportParam(reportParamKey2, "4"),
                new ReportParam(counterName3, "5")));
    }
    
}
