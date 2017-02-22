package eu.dnetlib.iis.wf.metadataextraction;

import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * Mock implementation of lock managing process.
 * 
 * @author mhorst
 *
 */
public class LockManagingProcessMock implements eu.dnetlib.iis.common.java.Process {

    @Override
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) throws Exception {
        // does nothing
    }

    @Override
    public Map<String, PortType> getInputPorts() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        return Collections.emptyMap();
    }

}
