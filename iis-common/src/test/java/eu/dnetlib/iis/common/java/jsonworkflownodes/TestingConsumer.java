package eu.dnetlib.iis.common.java.jsonworkflownodes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.common.TestsIOUtils;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.io.JsonUtils;
import eu.dnetlib.iis.common.java.jsonworkflownodes.PortSpecifications.SpecificationValues;
import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * Reads the data stores from specified input ports and compares them with
 * expected JSON-encoded data stores. If There is a mismatch,
 * an exception is thrown.
 *
 * @author Mateusz Kobos
 */
public class TestingConsumer implements Process {
	private final PortSpecifications inputSpecs;
	
	/**
	 * @param inputSpecifications specifications of input. Each element of
	 * the array corresponds to a single specification. Single specification
	 * conforms to the following template:
	 * "{input port name, schema reference, path to JSON file in resources 
	 * corresponding to the expected input data store}",
	 * e.g. "{person, eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person, 
	 * eu/dnetlib/iis/core/examples/person.json}"
	 */
	public TestingConsumer(String[] inputSpecifications){
		inputSpecs = new PortSpecifications(inputSpecifications);
	}
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return inputSpecs.getPortTypes();
	}

	@Override
	public Map<String, PortType> getOutputPorts() {
		return new HashMap<String, PortType>();
	}

	@Override
	public void run(PortBindings portBindings, Configuration configuration,
			Map<String, String> parameters) throws Exception {
		Map<String, Path> input = portBindings.getInput();
		FileSystem fs = FileSystem.get(configuration);
		for(Map.Entry<String, Path> e: input.entrySet()){
			SpecificationValues specs = inputSpecs.get(e.getKey());
			check(new FileSystemPath(fs, e.getValue()), specs);
		}
	}
	
	private static void check(FileSystemPath actualPath, SpecificationValues specs) throws IOException{
		List<SpecificRecord> expected = JsonUtils.convertToList(
				specs.jsonFilePath, specs.schema, SpecificRecord.class);
		List<SpecificRecord> actual = DataStore.read(actualPath, specs.schema);
		TestsIOUtils.assertEqualSets(expected, actual, true);
	}
}
