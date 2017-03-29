package eu.dnetlib.iis.common.java.jsonworkflownodes;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;

import eu.dnetlib.iis.common.java.jsonworkflownodes.StringPortSpecificationExtractor.PortSpecification;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.common.utils.AvroUtils;

/**
 * @author Mateusz Kobos
 */
public class PortSpecifications {
	private static final String[] propertyRegexps = 
			new String[]{"[\\w\\.]+", "[\\w\\./_\\-]+"};
	private final Map<String, SpecificationValues> specs;
	
    public static class SpecificationValues {

        private final Schema schema;

        private final String jsonFilePath;

        public SpecificationValues(Schema schema, String jsonFilePath) {
            this.schema = schema;
            this.jsonFilePath = jsonFilePath;
        }

        public Schema getSchema() {
            return schema;
        }

        public String getJsonFilePath() {
            return jsonFilePath;
        }

    }
	
	public PortSpecifications(String[] portSpecifications){
		StringPortSpecificationExtractor portSpecExtractor = 
				new StringPortSpecificationExtractor(propertyRegexps);
		specs = new HashMap<String, SpecificationValues>();
		for(int i = 0; i < portSpecifications.length; i++){
			PortSpecification portSpec = portSpecExtractor.getSpecification(portSpecifications[i]);
			Schema schema = AvroUtils.toSchema(portSpec.getProperties()[0]);
			String jsonPath = portSpec.getProperties()[1];
			specs.put(portSpec.getName(), new SpecificationValues(schema, jsonPath));
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
