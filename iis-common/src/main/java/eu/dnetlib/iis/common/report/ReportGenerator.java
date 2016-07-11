package eu.dnetlib.iis.common.report;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.common.schemas.ReportParam;

/**
 * Java workflow node process for building report.<br/>
 * It writes report properties into avro datastore of {@link ReportParam}s
 * with location specified in output port.<br/>
 * Report property name must start with <code>report.</code> to
 * be included in output datastore.
 * 
 * Usage example:<br/>
 * <pre>
 * {@code
 * <action name="report">
 *     <java>
 *         <main-class>eu.dnetlib.iis.common.java.ProcessWrapper</main-class>
 *         <arg>eu.dnetlib.iis.common.report.ReportGenerator</arg>
 *         <arg>-Preport.someProperty=someValue</arg>
 *         <arg>-Oreport=/report/path</arg>
 *     </java>
 *     ...
 * </action>
 * }
 * </pre>
 * Above example will produce avro datastore in <code>/report/path</code>
 * with single {@link ReportParam}.
 * Where the {@link ReportParam#getKey()} will be equal to <code>someProperty</code> and 
 * the {@link ReportParam#getValue()} will be equal to <code>someValue</code>
 * (notice the stripped <code>report.</code> prefix from the param key).
 * 
 * 
 * @author krzysztof
 *
 */
public class ReportGenerator implements Process {

    private static final String REPORT_PORT_OUT_NAME = "report";
    
    private static final String REPORT_PROPERTY_PREFIX = "report.";
    

    //------------------------ LOGIC --------------------------
    
    @Override
    public Map<String, PortType> getInputPorts() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        return Collections.singletonMap(REPORT_PORT_OUT_NAME, new AvroPortType(ReportParam.SCHEMA$));
    }
    
    @Override
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) throws Exception {
        
        Map<String, String> reportParams = collectReportParameters(parameters);
        
        List<ReportParam> avroReportParams = convertToAvroReportParams(reportParams);
        
        
        FileSystem fs = FileSystem.get(conf);
        
        Path reportPath = portBindings.getOutput().get(REPORT_PORT_OUT_NAME);
        
        DataStore.create(avroReportParams, new FileSystemPath(fs, reportPath));
        
    }
    
    private Map<String, String> collectReportParameters(Map<String, String> parameters) {
        
        return parameters.entrySet().stream()
                .filter(property -> property.getKey().startsWith(REPORT_PROPERTY_PREFIX))
                .map(x -> Pair.of(x.getKey().substring(REPORT_PROPERTY_PREFIX.length()), x.getValue()))
                .collect(Collectors.toMap(e -> e.getLeft(), e -> e.getRight()));
        
    }

    private List<ReportParam> convertToAvroReportParams(Map<String, String> reportParameters) {
        
        List<ReportParam> avroReportParams = Lists.newArrayList();
        reportParameters.forEach((key, value) -> avroReportParams.add(new ReportParam(key, value)));
        
        return avroReportParams;
    }
    

}
