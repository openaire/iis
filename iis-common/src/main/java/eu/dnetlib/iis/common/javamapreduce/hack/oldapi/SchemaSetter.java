package eu.dnetlib.iis.common.javamapreduce.hack.oldapi;

import java.lang.reflect.Field;

import org.apache.avro.Schema;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Mateusz Kobos
 */
public class SchemaSetter {
	public final static String inputClassName = 
			"eu.dnetlib.iis.avro.input.class";
	public final static String avroInput = "avro.input.schema";
	public final static String mapOutputKeyClassName = 
			"eu.dnetlib.iis.avro.map.output.key.class";
	public final static String mapOutputValueClassName = 
			"eu.dnetlib.iis.avro.map.output.value.class";
	public final static String avroMapOutput = "avro.map.output.schema";
	public final static String outputClassName = 
			"eu.dnetlib.iis.avro.output.class";
	public final static String avroOutput = "avro.output.schema";
	
	public final static String primitiveTypePrefix = 
			"org.apache.avro.Schema.Type.";
	
	public static void set(Configuration conf){
		if (conf == null) {
			return;
		}
		setSchemaString(conf, inputClassName, avroInput);
		setMapOutputSchemaString(conf);
		setSchemaString(conf, outputClassName, avroOutput);
	}
	
	private static void setSchemaString(
			Configuration conf,	
			String inputPropertyName, String outputPropertyName){
		String type = getPropertyValue(conf, inputPropertyName);
		String schemaString = getSchema(type).toString();		
		conf.set(outputPropertyName, schemaString);
	}
	
	private static void setMapOutputSchemaString(Configuration conf){
		String keyType = getPropertyValue(conf, mapOutputKeyClassName);
		Schema keySchema = getSchema(keyType);
		String valueType = getPropertyValue(conf, mapOutputValueClassName);
		Schema valueSchema = getSchema(valueType);
		String mapOutputSchema =  
				Pair.getPairSchema(keySchema, valueSchema).toString();
		conf.set(avroMapOutput, mapOutputSchema);
	}
	
	private static String getPropertyValue(
			Configuration conf, String propertyName){
		String value = conf.get(propertyName);
		if(value == null){
			throw new RuntimeException("Property "+
				"\""+propertyName+"\" is not defined");
		}
		return value;
	}
	private static Schema getSchema(String typeName) {
		Schema schema = null;
		if(typeName.startsWith(primitiveTypePrefix)){
			String shortName = typeName.substring(
					primitiveTypePrefix.length(), typeName.length());
			schema = getPrimitiveTypeSchema(shortName);
		} else {
			schema = getAvroClassSchema(typeName);
		}
		return schema;
	}
	
	private static Schema getPrimitiveTypeSchema(String shortName){
		Schema.Type type = Schema.Type.valueOf(shortName);
		Schema schema = Schema.create(type);
		return schema;
	}
	
	private static Schema getAvroClassSchema(String className){
		try {
			Class<?> avroClass = Class.forName(className);
			Field f = avroClass.getDeclaredField("SCHEMA$");
			Schema schema = (Schema) f.get(null);
			return schema;
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(
					"Class \""+className+"\" does not exist");
		} catch (SecurityException e) {
			throw new RuntimeException(e);
		} catch (NoSuchFieldException e) {
			throw new RuntimeException(e);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
}
