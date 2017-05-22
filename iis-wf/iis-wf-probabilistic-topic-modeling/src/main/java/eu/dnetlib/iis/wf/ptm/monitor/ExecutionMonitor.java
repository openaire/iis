package eu.dnetlib.iis.wf.ptm.monitor;

import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * @author mhorst
 *
 */
public class ExecutionMonitor implements eu.dnetlib.iis.common.java.Process {

    @Override
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) throws Exception {
        long timeoutMillis = Long.parseLong(parameters.get("timoutSecs")) * 1000;
        long sleepTimeSecs = Long.parseLong(parameters.get("sleepSecs"));
        long startTime = System.currentTimeMillis();
        long timePassedMillis;
        while ((timePassedMillis = (System.currentTimeMillis() - startTime)) < timeoutMillis) {
            Thread.sleep(sleepTimeSecs * 1000);
            System.out.println("time passed: " + timePassedMillis/1000 + " secs, waiting...");
        }
        System.out.println("time passed: " + timePassedMillis/1000 + " secs, finishing...");

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
