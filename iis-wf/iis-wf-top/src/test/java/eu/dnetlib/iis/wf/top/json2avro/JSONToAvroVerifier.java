package eu.dnetlib.iis.wf.top.json2avro;

import java.io.InputStream;
import java.security.InvalidParameterException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import eu.dnetlib.iis.common.java.io.JsonStreamReader;

/**
 * JSON to Avro verifier.
 * @author mhorst
 *
 */
public class JSONToAvroVerifier {

	public static void main(String[] args) throws Exception {
		if (args.length!=2) {
			throw new InvalidParameterException("expected parameters: "
					+ "args[0]: resource classpath location, "
					+ "args[1]: avro object full class name");
		}
		Schema inputSchema = (Schema) Class.forName(args[1]).getField("SCHEMA$").get(null);
		InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream(
				args[0]);
		JsonStreamReader<GenericRecord> reader = 
				new JsonStreamReader<GenericRecord>(
						inputSchema, input,	GenericRecord.class);
		try{
			while(reader.hasNext()){
				Object obj = reader.next();
				GenericRecord record = (GenericRecord) obj;
				System.out.println("got record" + record.toString());
			}
		} finally {
			if(reader != null){
				reader.close();
			}
		}	
	}

}
