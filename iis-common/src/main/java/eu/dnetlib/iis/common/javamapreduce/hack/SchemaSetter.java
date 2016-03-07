package eu.dnetlib.iis.common.javamapreduce.hack;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.iis.common.utils.AvroUtils;

/**
 * @author Mateusz Kobos
 */
public class SchemaSetter {
	public final static String inputClassName = 
			"eu.dnetlib.iis.avro.input.class";
	public final static String mapOutputKeyClassName = 
			"eu.dnetlib.iis.avro.map.output.key.class";
	public final static String mapOutputValueClassName = 
			"eu.dnetlib.iis.avro.map.output.value.class";
	public final static String outputClassName = 
			"eu.dnetlib.iis.avro.output.class";
	public final static String multipleOutputsPrefix = 
			"eu.dnetlib.iis.avro.multipleoutputs.class.";
	
	public final static String avroInput = "avro.schema.input.key";
	public final static String[] avroMapOutputKey = new String[]{
		"avro.serialization.key.reader.schema",
		"avro.serialization.key.writer.schema"};
	public final static String[] avroMapOutputValue = new String[]{
		"avro.serialization.value.reader.schema",
		"avro.serialization.value.writer.schema"};
	public final static String avroOutput = "avro.schema.output.key";
	public final static String avroMultipleOutputs = 
			"avro.mapreduce.multipleoutputs";
	public final static String avroMultipleOutputsPattern = 
			"avro.mapreduce.multipleoutputs.namedOutput.%s.keyschema";
	
	public static void set(Configuration conf){
		if (conf == null) {
			return;
		}
		setSchemaStrings(conf, inputClassName, new String[]{avroInput});
		setSchemaStrings(conf, mapOutputKeyClassName, avroMapOutputKey);
		setSchemaStrings(conf, mapOutputValueClassName, avroMapOutputValue);
		setSchemaStrings(conf, outputClassName, new String[]{avroOutput});
		handleMultipleOutputs(conf);
	}

	private static void handleMultipleOutputs(Configuration conf) {
		String outputNamesString = conf.get(avroMultipleOutputs);
		if(outputNamesString == null){
			return;
		}
		String[] outputNames = outputNamesString.trim().split(" ");
		for(String outputName: outputNames){
			String outputProperty = 
					String.format(avroMultipleOutputsPattern, outputName);
			setSchemaStrings(conf, multipleOutputsPrefix+outputName,
					new String[]{outputProperty});
			
		}
	}
	
	private static void setSchemaStrings(Configuration conf,
			String inputPropertyName, String[] outputPropertyNames){
		String type = getPropertyValue(conf, inputPropertyName);
		if(type == null){
			return;
		}
		String schemaString = AvroUtils.toSchema(type).toString();
		for(String property: outputPropertyNames){
			conf.set(property, schemaString);
		}
	}
	
	private static String getPropertyValue(
			Configuration conf, String propertyName){
		String value = conf.get(propertyName);
//		if(value == null){
//			throw new RuntimeException("Property "+
//				"\""+propertyName+"\" is not defined");
//		}
		return value;
	}
}
