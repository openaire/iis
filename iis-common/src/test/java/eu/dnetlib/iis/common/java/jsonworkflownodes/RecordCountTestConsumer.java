package eu.dnetlib.iis.common.java.jsonworkflownodes;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Maps;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.jsonworkflownodes.StringPortSpecificationExtractor.PortSpecification;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.common.utils.AvroUtils;

/**
 * Consumer of avro data stores.
 * It checks if size of provided data store fits in a provided range. 
 * 
 * @author madryk
 */
public class RecordCountTestConsumer implements Process {
	
	private final Map<String, RecordCountPortSpecification> inputPortsSpecification = Maps.newHashMap();
	
	
	//------------------------ CONSTRUCTORS --------------------------
	
	/**
	 * @param inputSpecifications specifications of input. Each element of
	 * the array corresponds to a single specification. Single specification
	 * conforms to the following template:
	 * "{input port name, schema reference, minimum records count in input avro data store, 
	 * maximum records count in input avro data store}",
	 * e.g. "{person, eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person, 10, 100}"
	 */
	public RecordCountTestConsumer(String[] inputSpecifications){
		StringPortSpecificationExtractor specificationExtractor = new StringPortSpecificationExtractor(new String[]{"[\\w\\.]+", "[\\d]+", "[\\d]+"});
		
		for (String inputSpecification : inputSpecifications) {
			PortSpecification portSpec = specificationExtractor.getSpecification(inputSpecification);
			Schema schema = AvroUtils.toSchema(portSpec.getProperties()[0]);
			int minRecords = Integer.valueOf(portSpec.getProperties()[1]);
			int maxRecords = Integer.valueOf(portSpec.getProperties()[2]);
			
			inputPortsSpecification.put(portSpec.getName(), new RecordCountPortSpecification(portSpec.getName(), schema, minRecords, maxRecords));
		}
		
		
	}
	
	
	//------------------------ GETTERS --------------------------
	
	@Override
	public Map<String, PortType> getInputPorts() {
		Map<String, PortType> inputPorts = Maps.newHashMap();
		for (Map.Entry<String, RecordCountPortSpecification> inputPortSpecification : inputPortsSpecification.entrySet()) {
			
			inputPorts.put(inputPortSpecification.getKey(), new AvroPortType(inputPortSpecification.getValue().getSchema()));
			
		}
		return inputPorts;
	}

	@Override
	public Map<String, PortType> getOutputPorts() {
		return new HashMap<String, PortType>();
	}


	//------------------------ LOGIC --------------------------
	
	@Override
	public void run(PortBindings portBindings, Configuration configuration,
			Map<String, String> parameters) throws Exception {
		Map<String, Path> input = portBindings.getInput();
		FileSystem fs = FileSystem.get(configuration);
		
		for(Map.Entry<String, Path> e: input.entrySet()){
			RecordCountPortSpecification specs = inputPortsSpecification.get(e.getKey());
			check(new FileSystemPath(fs, e.getValue()), specs);
		}
	}
	
	private static void check(FileSystemPath actualPath, RecordCountPortSpecification specs) throws IOException{
		
		List<SpecificRecord> actual = DataStore.read(actualPath, specs.getSchema());
		
		assertTrue("Expected at least " + specs.getMinimumRecordCount() + " records in " + specs.getName() + " avro data store, but was " + actual.size(), 
				actual.size() >= specs.getMinimumRecordCount());
		
		assertTrue("Expected at most " + specs.getMaximumRecordCount() + " records in " + specs.getName() + " avro data store, but was " + actual.size(), 
				actual.size() <= specs.getMaximumRecordCount());
	}
}
