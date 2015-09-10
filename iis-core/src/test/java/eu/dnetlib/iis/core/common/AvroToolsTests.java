package eu.dnetlib.iis.core.common;

import static org.junit.Assert.*;

import org.junit.Test;

public class AvroToolsTests {

	@Test
	public void testToSchemaPrimitiveType() {
		checkSchema("\"string\"", "org.apache.avro.Schema.Type.STRING");
	}
	
	@Test
	public void testToSchemaClassName(){
		checkSchema("{\"type\":\"record\",\"name\":\"Person\",\"namespace\":\"eu.dnetlib.iis.core.schemas.standardexamples\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}", 
				"eu.dnetlib.iis.core.schemas.standardexamples.Person");
	}
	
	private static void checkSchema(String expected, String className){
		String actual = AvroUtils.toSchema(className).toString();
		assertEquals(expected, actual);
	}

}
