package eu.dnetlib.iis.common.oozie.property;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.core.java.PortBindings;
import eu.dnetlib.iis.core.java.porttype.AnyPortType;
import eu.dnetlib.iis.core.java.porttype.PortType;

/**
 * Job properties dumper process. 
 * Writes all properties to job.properties file stored in output directory provided as input parameter.
 * 
 * @author mhorst
 *
 */
public class JobPropertiesDumperProcess implements eu.dnetlib.iis.core.java.Process {

	public static final String OUTPUT_PORT = "output";

	private static final Map<String, PortType> outputPorts = new HashMap<String, PortType>();
	
	{
		outputPorts.put(OUTPUT_PORT, new AnyPortType());
	}
	
	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters) throws Exception {
        Properties props = new Properties();
        for(Map.Entry<String,String> property : conf) {
        	props.setProperty(property.getKey(), 
        			property.getValue());
        }
		FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream outputStream = fs.create(new Path(
        				portBindings.getOutput().get(OUTPUT_PORT),
        				"job.properties"), true);
        try {
        	props.store(outputStream, "");	
        } finally {
        	outputStream.close();	
        }	
	}

	@Override
	public Map<String, PortType> getInputPorts() {
		return Collections.emptyMap();
	}

	@Override
	public Map<String, PortType> getOutputPorts() {
		return outputPorts;
	}
	
}
