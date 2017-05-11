package eu.dnetlib.iis.common.java;

import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * @author mhorst
 *
 */
public class TestProcess implements Process {

    public static boolean executed;
    
    @Override
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters)
            throws Exception {
        executed = true;
    }

    @Override
    public Map<String, PortType> getInputPorts() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        return Collections.emptyMap();
    }

    public static boolean isExecuted() {
        return executed;
    }
    
}
