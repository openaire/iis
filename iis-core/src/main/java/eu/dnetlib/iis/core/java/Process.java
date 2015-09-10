package eu.dnetlib.iis.core.java;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.iis.core.java.porttype.PortType;

/** Workflow node written in Java.
 * 
 * The implementing class has to define a constructor with no parameters 
 * (possibly the default one) or a constructor with String[] as a single
 * parameter.
 * @author Mateusz Kobos
 */
public interface Process {
	/**
	 * Run the process.
	 * 
	 * The process ends with a success status if no exception is thrown, 
	 * otherwise it ends with an error status.
	 *
	 * @param parameters parameters of the process. Each parameter
	 * corresponds to a single entry in the map, its name is the key, its
	 * value is the value.
	 * @throws Exception if thrown, it means that the process finished
	 * with an error status
	 */
	void run(PortBindings portBindings, Configuration conf, 
			Map<String, String> parameters) throws Exception;
	
	/**
	 * @return map containing as the key: name of the port, as the value: type
	 * of the port 
	 */
	Map<String, PortType> getInputPorts();
	
	/**
	 * @return map containing as the key: name of the port, as the value: type
	 * of the port 
	 */
	Map<String, PortType> getOutputPorts();
}