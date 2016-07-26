package eu.dnetlib.iis.common.report;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.counter.PigCounters;
import eu.dnetlib.iis.common.schemas.ReportParam;

/**
 * Resolver of {@link ReportParam}s from {@link PigCounters} that uses {@link ReportPigCounterMapping}.
 * 
 * @author madryk
 */
public class ReportPigCountersResolver {

    //------------------------ LOGIC --------------------------
    
    /**
     * Resolve {@link ReportParam}s from {@link PigCounters} using {@link ReportPigCounterMapping}s.
     * Only counters that are present in {@link ReportPigCounterMapping}s will be resolved.
     */
    public List<ReportParam> resolveReportCounters(PigCounters pigCounters, List<ReportPigCounterMapping> reportPigCountersMapping) {
        
        List<ReportParam> reportCounters = Lists.newArrayList();
        
        for (ReportPigCounterMapping counterMapping : reportPigCountersMapping) {
            
            String jobId = pigCounters.getJobIdByAlias(counterMapping.getSourcePigJobAlias());
            
            if (jobId == null) {
                throw new IllegalArgumentException("Invalid job alias: " + counterMapping.getSourcePigJobAlias());
            }
            
            String counterValue = pigCounters.getJobCounters(jobId).getCounter(counterMapping.getSourcePigJobCounterName());
            
            if (StringUtils.isBlank(counterValue)) {
                throw new IllegalArgumentException("Couldn't find a counter with name: " + counterMapping.getSourcePigJobCounterName() + " inside job counters, id: " + jobId);
            }
            
            reportCounters.add(new ReportParam(counterMapping.getDestReportCounterName(), counterValue));
        }
        
        return reportCounters;
    }
}
