package eu.dnetlib.iis.common.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import eu.dnetlib.iis.core.java.PortBindings;
import eu.dnetlib.iis.core.java.Ports;
import eu.dnetlib.iis.core.java.Process;
import eu.dnetlib.iis.core.java.io.CloseableIterator;
import eu.dnetlib.iis.core.java.io.DataStore;
import eu.dnetlib.iis.core.java.io.FileSystemPath;
import eu.dnetlib.iis.core.java.porttype.AnyPortType;
import eu.dnetlib.iis.core.java.porttype.PortType;

/**
 * Simple process verifying whether given datastore is empty.
 * @author mhorst
 *
 */
public class EmptyDatastoreVerifierProcess implements Process {

	public static final String INPUT_PORT_NAME = "input";
	
	public static final String OOZIE_ACTION_OUTPUT_FILENAME = "oozie.action.output.properties";
	
	public static final String DEFAULT_ENCODING = "UTF-8";
	
	public static final String OUTPUT_PROPERTY_IS_EMPTY = "isEmpty";
	
	/**
	 * Ports handled by this module.
	 */
	private static final Ports ports;
	
	static {
//		preparing ports
		Map<String, PortType> input = new HashMap<String, PortType>();
		input.put(INPUT_PORT_NAME, new AnyPortType());
		Map<String, PortType> output = Collections.emptyMap();
		ports = new Ports(input, output);
	}
	
	/**
	 * Returns ports in a static way.
	 * @return
	 */
	static protected Ports getStaticPorts() {
		return ports;
	}
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return getStaticPorts().getInput();
	}

	@Override
	public Map<String, PortType> getOutputPorts() {
		return getStaticPorts().getOutput();
	}

	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters) throws Exception {
		CloseableIterator<?> closeableIt = DataStore.getReader(
				new FileSystemPath(FileSystem.get(conf), 
						portBindings.getInput().get(INPUT_PORT_NAME)));
		try {
			File file = new File(System.getProperty(OOZIE_ACTION_OUTPUT_FILENAME));
	        Properties props = new Properties();
			props.setProperty(OUTPUT_PROPERTY_IS_EMPTY, 
	        		Boolean.toString(!closeableIt.hasNext()));	
			OutputStream os = new FileOutputStream(file);
	        try {
		        	props.store(os, "");	
	        } finally {
	        	os.close();	
	        }	
		} finally {
			closeableIt.close();
		}
	}

}
