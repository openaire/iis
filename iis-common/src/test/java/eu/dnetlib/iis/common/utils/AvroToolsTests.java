package eu.dnetlib.iis.common.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class AvroToolsTests {

	@Test
	public void testToSchemaPrimitiveType() {
		checkSchema("\"string\"", "org.apache.avro.Schema.Type.STRING");
	}
	
	@Test
	public void testToSchemaClassName(){
		checkSchema("{\"type\":\"record\",\"name\":\"Person\",\"namespace\":\"eu.dnetlib.iis.common.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}", 
				"eu.dnetlib.iis.common.avro.Person");
	}
	
	private static void checkSchema(String expected, String className){
		String actual = AvroUtils.toSchema(className).toString();
		assertEquals(expected, actual);
	}

}
