package eu.dnetlib.iis.common.utils;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.OOZIE_ACTION_OUTPUT_FILENAME;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.InvalidParameterException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Ports;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.CloseableIterator;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AnyPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * Simple process verifying whether given datastore is empty.
 * @author mhorst
 *
 */
public class EmptyDatastoreVerifierProcess implements Process {

	public static final String INPUT_PORT_NAME = "input";
	
	public static final String DEFAULT_ENCODING = "UTF-8";
	
	public static final String OUTPUT_PROPERTY_IS_EMPTY = "isEmpty";

    /**
     * Ports handled by this module.
     */
    private final Ports ports;

	
	// ------------------------ CONSTRUCTORS --------------------------
	
	public EmptyDatastoreVerifierProcess() {
//      preparing ports
        Map<String, PortType> input = new HashMap<String, PortType>();
        input.put(INPUT_PORT_NAME, new AnyPortType());
        Map<String, PortType> output = Collections.emptyMap();
        ports = new Ports(input, output); 
	}
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return ports.getInput();
	}

	@Override
	public Map<String, PortType> getOutputPorts() {
		return ports.getOutput();
	}

	@Override
	public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) throws Exception {
	    if (!portBindings.getInput().containsKey(INPUT_PORT_NAME)) {
	        throw new InvalidParameterException("missing input port!");
	    }
	    
		try (CloseableIterator<?> closeableIt = getIterator(conf, portBindings.getInput().get(INPUT_PORT_NAME))) {
			File file = new File(System.getProperty(OOZIE_ACTION_OUTPUT_FILENAME));
	        Properties props = new Properties();
			props.setProperty(OUTPUT_PROPERTY_IS_EMPTY, Boolean.toString(!closeableIt.hasNext()));	
	        try (OutputStream os = new FileOutputStream(file)) {
		        	props.store(os, "");	
	        }
		}
	}
	
	/**
	 * Returns iterator over datastore.
	 */
	protected CloseableIterator<?> getIterator(Configuration conf, Path path) throws IOException {
	    return DataStore.getReader(new FileSystemPath(FileSystem.get(conf), path));
	}

}
