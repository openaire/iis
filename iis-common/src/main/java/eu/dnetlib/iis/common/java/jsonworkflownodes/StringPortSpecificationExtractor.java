package eu.dnetlib.iis.common.java.jsonworkflownodes;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts information about port name and its properties from a string
 * of a form "{port_name, property_1, property_2, ...}"
 * @author Mateusz Kobos
 */
public class StringPortSpecificationExtractor {
	private final String portNameRegexp;
	private final String[] propertiesRegexp;
	private final String portSpecificationRegexp;
	private final Pattern pattern;

	public static class PortSpecification{
		public String name;
		public String[] properties;
		
		public PortSpecification(String name, String[] properties) {
			this.name = name;
			this.properties = properties;
		}
	}
	
	/**
	 * @param propertiesRegexp regular expressions specifying pattern for 
	 * each of the properties associated with a port. An example of a single 
	 * specification: {@code "[\\w\\.]+"}.
	 */
	public StringPortSpecificationExtractor(String[] propertiesRegexp){
		this.portNameRegexp = "[\\w\\._]+";
		this.propertiesRegexp = propertiesRegexp;
		this.portSpecificationRegexp = createRegexpString(portNameRegexp, propertiesRegexp);
		this.pattern = Pattern.compile(this.portSpecificationRegexp);
	}

	private static String createRegexpString(String portNameRegexp, String[] propertiesRegexp){
		StringBuilder regexp = new StringBuilder();
		regexp.append("s*\\{\\s*");
		regexp.append("("+portNameRegexp+")");
		for(String propertyRegexp: propertiesRegexp){
			regexp.append(",\\s*("+propertyRegexp+")");
		}
		regexp.append("\\s*\\}\\s*");
		return regexp.toString();
	}
	
	private int getPropertiesCount(){
		return propertiesRegexp.length;
	}
	
	public PortSpecification getSpecification(String text){
		Matcher m = pattern.matcher(text);
		final int expectedGroupsCount = getPropertiesCount()+1;
		if(!m.matches()){
			throw new RuntimeException(String.format("Specification of " +
					"the port (\"%s\") does not match regexp \"%s\"", 
					text, portSpecificationRegexp));
		}
		if(m.groupCount() != expectedGroupsCount){
			StringBuilder groups = new StringBuilder();
			for(int i = 0; i < m.groupCount(); i++){
				groups.append("\""+m.group(i)+"\"");
				if(i != m.groupCount()-1) groups.append(", ");
			}
			throw new RuntimeException(String.format(
				"Invalid output port specification \"%s\": got %d groups "+
				"instead of %d (namely: %s)", text, m.groupCount(), 
				expectedGroupsCount, groups.toString()));
		}
		String[] properties = new String[getPropertiesCount()];
		for(int i = 0; i < getPropertiesCount(); i++){
			properties[i] = m.group(i+2);
		}
		return new PortSpecification(m.group(1), properties);
	}
}
