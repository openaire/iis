package eu.dnetlib.iis.common.report;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.common.counter.PigCounters;
import eu.dnetlib.iis.common.counter.PigCountersParser;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.common.schemas.ReportEntry;

/**
 * Java workflow node process for building report from pig counters.<br/>
 * <br/>
 * It writes report properties into avro datastore of {@link ReportEntry}s
 * with location specified in output port.<br/>
 * Report property key must start with <code>report.</code> to
 * be included in output datastore.<br/>
 * <br/>
 * Report property values can contain placeholders for easier evaluation of
 * pig counters. Placeholders are resolved using {@link PigCounterValueResolver}.<br/>
 * <br/>
 * Process needs <code>pigCounters</code> property that contains json representation
 * of pig counters for working.<br/>
 * 
 * @author madryk
 */
public class PigCountersReportGenerator implements Process {

    private static final String REPORT_PORT_OUT_NAME = "report";
    
    private static final String REPORT_PROPERTY_PREFIX = "report.";
    
    private static final String PIG_COUNTERS_PROPERTY = "pigCounters";
    
    
    private PigCountersParser pigCountersParser = new PigCountersParser();
    
    private ReportPigCounterMappingParser reportPigCounterMappingParser = new ReportPigCounterMappingParser();
    
    private ReportPigCountersResolver reportPigCountersResolver = new ReportPigCountersResolver();
    
    
    //------------------------ LOGIC --------------------------
    
    @Override
    public Map<String, PortType> getInputPorts() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        return Collections.singletonMap(REPORT_PORT_OUT_NAME, new AvroPortType(ReportEntry.SCHEMA$));
    }
    
    @Override
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) throws Exception {
        
        String pigCountersJson = parameters.get(PIG_COUNTERS_PROPERTY);
        
        PigCounters pigCounters = pigCountersParser.parse(pigCountersJson);
        
        
        List<ReportPigCounterMapping> reportCountersMapping = collectReportCountersMapping(parameters);
        
        List<ReportEntry> reportCounters = reportPigCountersResolver.resolveReportCounters(pigCounters, reportCountersMapping);
        
        
        FileSystem fs = FileSystem.get(conf);
        
        Path reportPath = portBindings.getOutput().get(REPORT_PORT_OUT_NAME);
        
        DataStore.create(reportCounters, new FileSystemPath(fs, reportPath));
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private List<ReportPigCounterMapping> collectReportCountersMapping(Map<String, String> parameters) {
        
        return parameters.entrySet().stream()
                .filter(property -> property.getKey().startsWith(REPORT_PROPERTY_PREFIX))
                .map(x -> Pair.of(x.getKey().substring(REPORT_PROPERTY_PREFIX.length()), x.getValue()))
                .map(x -> reportPigCounterMappingParser.parse(x.getKey(), x.getValue()))
                .collect(Collectors.toList());
        
    }
    
}
