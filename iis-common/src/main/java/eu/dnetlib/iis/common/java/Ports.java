package eu.dnetlib.iis.common.java;

import java.util.Map;

import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * A class that groups information about input and output ports, i.e.
 * their (name of the port -> type of the port) mappings. 
 * @author Mateusz Kobos
 */
public class Ports {
	private final Map<String, PortType> input;
	private final Map<String, PortType> output;
	
	public Ports(Map<String, PortType> input, Map<String, PortType> output){
		this.input = input;
		this.output = output;
	}

	public Map<String, PortType> getInput() {
		return input;
	}
	public Map<String, PortType> getOutput() {
		return output;
	}
}
