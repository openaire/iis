package eu.dnetlib.iis.common.java;

import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.Path;

/**
 * Port names (see {@link Ports}) bound to certain paths in the file system
 * @author Mateusz Kobos
 *
 */
public class PortBindings {
	private Map<String, Path> input;
	private Map<String, Path> output;

	public PortBindings(Map<String, Path> input, Map<String, Path> output) {
		this.input = input;
		this.output = output;
	}
	
	public Map<String, Path> getInput() {
		return input;
	}
	
	public Map<String, Path> getOutput() {
		return output;
	}
	
	@Override
	public boolean equals(Object o){
		if(!(o instanceof PortBindings)){
			return false;
		}
		PortBindings other = (PortBindings) o;
		return input.equals(other.input) && output.equals(other.output);
	}
	
	@Override
	public int hashCode(){
		throw new NotImplementedException();
	}
}
