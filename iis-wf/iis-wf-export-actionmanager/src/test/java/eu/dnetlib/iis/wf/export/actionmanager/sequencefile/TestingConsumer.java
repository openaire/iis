package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import eu.dnetlib.iis.common.StaticResourceProvider;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.iis.common.OrderedProperties;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.io.SequenceFileTextValueReader;
import eu.dnetlib.iis.common.java.porttype.AnyPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.wf.export.actionmanager.entity.AtomicActionSerDeUtils;

/**
 * Sequence file testing consumer. Expects {@link Text} values at input.
 * Requirements are provided as comma separated list of properties files 
 * holding requirements for each individual input object. 
 * Properties keys are paths to the fields we want to test in given object, 
 * properties values are expected values (with special markup for null: $NULL$).
 * This means number of input records in sequence file should be equal to the number of provided properties files.
 *   
 * @author mhorst
 */
public class TestingConsumer implements Process {

	public static final String EXPECTATION_FILE_PATHS = "expectation_file_paths";

	public static final String PORT_INPUT = "seqfile";

	public static final String NULL_VALUE_INDICATOR = "$NULL$";

	private final FieldAccessor accessor;
	
	private final static Logger log = Logger.getLogger(TestingConsumer.class);

	private final Map<String, PortType> inputPorts = new HashMap<String, PortType>();
	
	
	//------------------------ CONSTRUCTORS --------------------------
	
	public TestingConsumer() {
	    inputPorts.put(PORT_INPUT, new AnyPortType());
		accessor = new FieldAccessor();
	}

	//------------------------ LOGIC ---------------------------------
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return inputPorts;
	}

	@Override
	public Map<String, PortType> getOutputPorts() {
		return Collections.emptyMap();
	}

	@Override
	public void run(PortBindings portBindings, Configuration configuration, Map<String, String> parameters)
			throws Exception {
		String propertiesPathsCSV = parameters.get(EXPECTATION_FILE_PATHS);
		if (StringUtils.isEmpty(propertiesPathsCSV)) {
			throw new Exception("no " + EXPECTATION_FILE_PATHS + " property value provided, "
					+ "output requirements were not specified!");
		}
		
		FileSystem fs = FileSystem.get(configuration);
		Path inputPath = portBindings.getInput().get(PORT_INPUT);
		if (!fs.exists(inputPath)) {
			throw new Exception(inputPath + " hdfs location does not exist!");
		}
		try (SequenceFileTextValueReader it = new SequenceFileTextValueReader(
				new FileSystemPath(fs, inputPath))) {

		    int actionsCount = 0;
			String[] recordsSpecs = StringUtils.split(propertiesPathsCSV, ',');

			while (it.hasNext()) {
			    String serializedAction = it.next().toString();
				AtomicAction<? extends Oaf> action = AtomicActionSerDeUtils.deserializeAction(serializedAction);
				actionsCount++;
				if (actionsCount > recordsSpecs.length) {
					throw new Exception("got more records than expected: " + "unable to verify record no " + actionsCount
							+ ", no field specification provided! Record contents: " + serializedAction);
				} else {
					evaluateExpectations(recordsSpecs[actionsCount - 1], action, serializedAction);
				}
			}
			if (actionsCount < recordsSpecs.length) {
				throw new Exception(
						"records count mismatch: " + "got: " + actionsCount + " expected: " + recordsSpecs.length);
			}
		}
	}
	
    //------------------------ PRIVATE ---------------------------------
	
	private void evaluateExpectations(String currentSpecLocation, AtomicAction<? extends Oaf> action, String serializedAction) throws Exception {
        log.info("output specification location: " + currentSpecLocation);
        Properties specProps = new OrderedProperties();
		specProps.load(StaticResourceProvider.getResourceInputStream(currentSpecLocation.trim()));
        Iterator<Entry<Object,Object>> propsIter = specProps.entrySet().iterator();
        while (propsIter.hasNext()) {
            Entry<Object,Object> entry = propsIter.next();
            Object currentValue = accessor.getValue((String)entry.getKey(), action);
            if ((currentValue != null && !entry.getValue().equals(currentValue.toString())) 
                    || (currentValue == null && !NULL_VALUE_INDICATOR.equals(entry.getValue()))) {
                throw new Exception("invalid field value for path: " + entry.getKey()
                                + ", expected: '" + entry.getValue() + "', "
                                + "got: '" + currentValue + "' Full object content: " + serializedAction);
            }
        }
	}
}