package eu.dnetlib.iis.common.oozie.property;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.porttype.AnyPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * Oozie properties dumper process. 
 * Writes all oozie parameters to job.properties file stored in output directory provided as input parameter.
 * 
 * @author mhorst
 *
 */
public class OoziePropertiesDumperProcess implements eu.dnetlib.iis.common.java.Process {

	public static final String OUTPUT_PORT = "output";

	private static final Map<String, PortType> outputPorts = new HashMap<String, PortType>();
	
	{
		outputPorts.put(OUTPUT_PORT, new AnyPortType());
	}
	
	@Override
	public void run(PortBindings portBindings, Configuration jobConf,
			Map<String, String> parameters) throws Exception {
        
        Configuration actionConf = new Configuration(false);
        String actionConfLocation = System.getProperty("oozie.action.conf.xml");
        System.out.println("loading properties from: " + actionConfLocation);
        actionConf.addResource(new Path("file:///", actionConfLocation)); 
        
        Properties props = new Properties();
        for(Map.Entry<String,String> property : actionConf) {
        	props.setProperty(property.getKey(), 
        			property.getValue());
        }
		FileSystem fs = FileSystem.get(actionConf);
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
