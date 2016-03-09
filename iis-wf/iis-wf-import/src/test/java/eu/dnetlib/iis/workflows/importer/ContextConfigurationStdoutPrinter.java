package eu.dnetlib.iis.workflows.importer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * Simple context configuration stdout printer.
 * @author mhorst
 *
 */
public class ContextConfigurationStdoutPrinter implements eu.dnetlib.iis.common.java.Process {

	/**
	 * Encoding to be used when building identifiers from byte[].
	 */
	protected String encoding = "utf-8"; 
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return new HashMap<String, PortType>();
	}
	
	@Override
	public Map<String, PortType> getOutputPorts() {
		return new HashMap<String, PortType>();
	}

	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters) throws Exception {
		
		System.out.println("listing hadoop context configuration properties");
		Iterator<Entry<String,String>> ctxIt = conf.iterator();
		while(ctxIt.hasNext()) {
			Entry<String,String> entry = ctxIt.next();
			System.out.println(entry.getKey() + "=" + entry.getValue());
		}
		System.out.println("end of hadoop context configuration properties");
	}
	
}
