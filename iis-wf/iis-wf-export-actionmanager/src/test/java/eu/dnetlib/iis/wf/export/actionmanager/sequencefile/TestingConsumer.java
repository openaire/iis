package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.io.SequenceFileTextValueReader;
import eu.dnetlib.iis.common.java.porttype.AnyPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;

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

	public static final String PROPERTIES_CSV = "expectation_properties_csv";

	public static final String PORT_INPUT = "seqfile";

	public static final String NULL_VALUE_INDICATOR = "$NULL$";

	private final FieldAccessor accessor;
	
	private final static Logger log = Logger.getLogger(TestingConsumer.class);

	private static final Map<String, PortType> inputPorts = new HashMap<String, PortType>();
	
	{
		inputPorts.put(PORT_INPUT, new AnyPortType());
	}
	
	//------------------------ CONSTRUCTORS --------------------------
	
	public TestingConsumer() {
		accessor = new FieldAccessor();
		accessor.registerDecoder("targetValue", new OafFieldDecoder());
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
		Path inputPath = portBindings.getInput().get(PORT_INPUT);
		String propertiesPathsCSV = parameters.get(PROPERTIES_CSV);
		if (StringUtils.isEmpty(propertiesPathsCSV)) {
			throw new Exception("no " + PROPERTIES_CSV + " property value provided, "
					+ "output requirements were not specified!");
		}
		String[] recordsSpecs = StringUtils.split(propertiesPathsCSV, ',');
		FileSystem fs = FileSystem.get(configuration);
		if (!fs.exists(inputPath)) {
			throw new Exception(inputPath + " hdfs location does not exist!");
		}
		try (SequenceFileTextValueReader it = new SequenceFileTextValueReader(
				new FileSystemPath(fs, inputPath))) {
			int actionsCount = 0;
			while (it.hasNext()) {
				AtomicAction action = AtomicAction.fromJSON(it.next().toString());
				actionsCount++;
				if (actionsCount > recordsSpecs.length) {
					throw new Exception("got more records than expected: " + "unable to verify record no " + actionsCount
							+ ", no field specification provided! Record contents: " + action);
				} else {
					String currentSpecLocation = recordsSpecs[actionsCount - 1];
					log.info("output specification location: " + currentSpecLocation);
					Properties specProps = new OrderedProperties();
					specProps.load(TestingConsumer.class.getResourceAsStream(
							currentSpecLocation.trim()));
					Iterator<Entry<Object,Object>> propsIter = specProps.entrySet().iterator();
					while (propsIter.hasNext()) {
						Entry<Object,Object> entry = propsIter.next();
						Object currentValue = accessor.getValue((String)entry.getKey(), action);
						if ((currentValue != null && !entry.getValue().equals(currentValue.toString())) 
								|| (currentValue == null && !NULL_VALUE_INDICATOR.equals(entry.getValue()))) {
							throw new Exception(
									"invalid field value for path: " + entry.getKey()
											+ ", expected: '" + entry.getValue() + "', "
											+ "got: '" + currentValue + "' Full object content: " + action);
						}
					}
				}
			}
			if (actionsCount < recordsSpecs.length) {
				throw new Exception(
						"records count mismatch: " + "got: " + actionsCount + " expected: " + recordsSpecs.length);
			}
		}
	}
}