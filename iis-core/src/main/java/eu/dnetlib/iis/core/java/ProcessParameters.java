package eu.dnetlib.iis.core.java;

import java.util.Map;

import org.apache.commons.lang.NotImplementedException;

/**
 * Parameters of the Process retrieved from Oozie
 * @author Mateusz Kobos
 *
 */
public class ProcessParameters {
	private final PortBindings portBindings;
	private final Map<String, String> parameters;
	
	public PortBindings getPortBindings() {
		return portBindings;
	}
	
	public Map<String, String> getParameters(){
		return parameters;
	}

	public ProcessParameters(PortBindings portBindings, 
			Map<String, String> parameters) {
		this.portBindings = portBindings;
		this.parameters =  parameters;
	}
	
	@Override
	public boolean equals(Object o){
		if(!(o instanceof ProcessParameters)){
			return false;
		}
		ProcessParameters other = (ProcessParameters) o;
		return this.portBindings.equals(other.portBindings);
	}
	
	@Override
	public int hashCode(){
		throw new NotImplementedException();
	}
}
