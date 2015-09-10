package eu.dnetlib.iis.core.java.jsonworkflownodes;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;

import eu.dnetlib.iis.core.common.AvroUtils;
import eu.dnetlib.iis.core.java.jsonworkflownodes.StringPortSpecificationExtractor.PortSpecification;
import eu.dnetlib.iis.core.java.porttype.AvroPortType;
import eu.dnetlib.iis.core.java.porttype.PortType;

/**
 * @author Mateusz Kobos
 */
public class PortSpecifications {
	private static final String[] propertyRegexps = 
			new String[]{"[\\w\\.]+", "[\\w\\./_\\-]+"};
	private final Map<String, SpecificationValues> specs;
	
	public static class SpecificationValues{
		public Schema schema;
		public String jsonFilePath;
		
		public SpecificationValues(Schema schema, String jsonFilePath) {
			this.schema = schema;
			this.jsonFilePath = jsonFilePath;
		}
	}
	
	public PortSpecifications(String[] portSpecifications){
		StringPortSpecificationExtractor portSpecExtractor = 
				new StringPortSpecificationExtractor(propertyRegexps);
		specs = new HashMap<String, SpecificationValues>();
		for(int i = 0; i < portSpecifications.length; i++){
			PortSpecification portSpec = portSpecExtractor.getSpecification(portSpecifications[i]);
			Schema schema = AvroUtils.toSchema(portSpec.properties[0]);
			String jsonPath = portSpec.properties[1];
			specs.put(portSpec.name, new SpecificationValues(schema, jsonPath));
		}
	}
	
	public SpecificationValues get(String portName){
		return specs.get(portName);
	}
	
	public Map<String, PortType> getPortTypes(){
		Map<String, PortType> ports = new HashMap<String, PortType>();
		for(Map.Entry<String, SpecificationValues> e: specs.entrySet()){
			Schema schema = e.getValue().schema;
			ports.put(e.getKey(), new AvroPortType(schema));
		}
		return ports;	
	}
}
