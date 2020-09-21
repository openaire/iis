package eu.dnetlib.iis.common.java.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

/**
 * 
 * @author Mateusz Kobos
 * 
 */
public final class JsonUtils {
    
    //------------------------ CONSTRUCTORS -------------------
    
    private JsonUtils() {}
    
    //------------------------ LOGIC --------------------------
    
	/**
	 * Read JSON file divided into lines, where each one corresponds to a
	 * record. Next, save the extracted records in a data store.
	 */
	public static void convertToDataStore(Schema inputSchema,
			InputStream input, FileSystemPath outputPath) throws IOException {
		JsonStreamReader<GenericRecord> reader = new JsonStreamReader<GenericRecord>(
				inputSchema, input, GenericRecord.class);
		DataFileWriter<GenericRecord> writer = DataStore.create(outputPath,
				inputSchema);
		try {
			while (reader.hasNext()) {
				Object obj = reader.next();
				GenericRecord record = (GenericRecord) obj;
				writer.append(record);
			}
		} finally {
			if (writer != null) {
				writer.close();
			}
			if (reader != null) {
				reader.close();
			}
		}
	}

	/**
	 * Read given JSON file from resources and convert it to a list of data structures. A utility function.
	 */
	public static <T> List<T> convertToList(String resourcesJsonFilePath,
			Schema schema, Class<T> type) {
		InputStream in = ClassPathResourceProvider.getResourceInputStream(resourcesJsonFilePath);
		return convertToList(in, schema, type);
	}

	/**
	 * Convert JSON data from a string (one record per line) to a list of data
	 * structures.
	 */
	public static <T> List<T> convertToListFromString(String data,
			Schema schema, Class<T> type) {
		InputStream in = new ByteArrayInputStream(data.getBytes());
		return convertToList(in, schema, type);
	}

	/**
	 * Read JSON data from input stream and convert it to a list of data
	 * structures.
	 */
	public static <T> List<T> convertToList(InputStream inputStream,
			Schema schema, Class<T> type) {
		try {
		    try (JsonStreamReader<T> reader = new JsonStreamReader<T>(schema, inputStream, type)) {
		        ArrayList<T> list = new ArrayList<T>();
	            while (reader.hasNext()) {
	                list.add(reader.next());
	            }
	            return list;    
		    }
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Convert an ugly JSON string to a "pretty" version (with indentation)
	 */
	public static String toPrettyJSON(String uglyJson) {
		Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
		JsonParser jp = new JsonParser();
		JsonElement je = jp.parse(uglyJson);
		return gson.toJson(je);
	}
}
