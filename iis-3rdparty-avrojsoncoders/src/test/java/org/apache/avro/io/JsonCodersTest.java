package org.apache.avro.io;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.schemas.Document;
import org.apache.avro.io.schemas.PersonEntry;
import org.apache.avro.io.schemas.PersonWithDocument;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Mateusz Kobos
 */
public class JsonCodersTest {

	@Test
	public void testJsonSchemaSimpleWithNull() throws IOException {
		checkJsonReadWrite("{\"age\":10}\n{\"age\":null}",
				getCertainPersonNullableSchema());
	}

	@Test
	public void testJsonSchemaWithoutNull() throws IOException {
		checkJsonReadWrite("{\"age\":10}\n{\"age\":33}",
				getCertainPersonSchema());
	}

	@Test
	public void testJsonSchemaWritingNullToNonNullableField()
			throws IOException {
		checkJsonReadWrite("{\"age\":10}\n{\"age\":null}",
				getCertainPersonSchema(), true);
	}

	@Test
	public void testSimpleNullableField() throws IOException {
		checkJsonReadWrite(
				"{\"id\":1,\"title\":\"Interesting stuff\",\"authorIds\":[1,2,3]}",
				Document.SCHEMA$);
		checkJsonReadWrite("{\"id\":1,\"title\":null,\"authorIds\":[1,2,3]}",
				Document.SCHEMA$);
		// checkJsonReadWrite("{\"id\":1,\"authorIds\":[1,2,3]}", Document.SCHEMA$);
		checkJsonReadWrite("{\"id\":1,\"title\":\"Interesting stuff\"}",
				Document.SCHEMA$, true);
	}

	@Test
	public void testNestedStructures() throws IOException {
		String records = readFromResources(
				"org/apache/avro/io/nested_data.json");
		checkJsonReadWrite(records, PersonWithDocument.SCHEMA$);
	}

	/**
	 * When null is not one of the elements in the union, you have to specify
	 * the type of the elements in the union explicitly
	 */
	@Test
	public void testUnionWithoutNull() throws IOException {
		String records = readFromResources(
				"org/apache/avro/io/union_without_null.json");
		checkJsonReadWrite(records, PersonEntry.SCHEMA$);
		checkJsonReadWrite("{\"id\":22,\"externalId\":3}", PersonEntry.SCHEMA$,
				true);
	}

	private static String readFromResources(String path) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(Thread
				.currentThread().getContextClassLoader()
				.getResourceAsStream(path)));
		StringBuffer buffer = new StringBuffer();
		boolean isFirst = true;
		for (String line = reader.readLine(); line != null; line = reader
				.readLine()) {
			if (isFirst) {
				isFirst = false;
			} else {
				buffer.append("\n");
			}
			isFirst = false;
			buffer.append(line);
		}
		return buffer.toString();
	}

	private void checkJsonReadWrite(String jsonInput, Schema schema)
			throws IOException {
		checkJsonReadWrite(jsonInput, schema, false);
	}

	/**
	 * Convert given JSON input string to Avro records, then convert it back to
	 * JSON and compare it with the original.
	 */
	private void checkJsonReadWrite(String jsonInput, Schema schema,
			boolean shouldThrowParsingException) throws IOException {
		try {
			List<GenericRecord> outRecords = toRecords(jsonInput, schema,
					schema);
			String jsonOutput = toJson(outRecords, schema);
			assertEquals(jsonInput, jsonOutput);
		} catch (AvroTypeException ex) {
			if (shouldThrowParsingException) {
				return;
			} else {
				throw new RuntimeException(ex);
			}
		}
		if (shouldThrowParsingException) {
			fail("This code should not have been reached because of previous "
					+ "exception thrown");
		}
	}

	private static Schema getCertainPersonSchema() throws IOException {
		return new Schema.Parser().parse(readFromResources(
				"org/apache/avro/io/schemas/certain_person.json"));
	}

	private static Schema getCertainPersonNullableSchema() throws IOException {
		return new Schema.Parser().parse(readFromResources(
				"org/apache/avro/io/schemas/certain_person_nullable.json"));
	}

	private static String toJson(List<GenericRecord> records, Schema schema)
			throws IOException {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		HackedJsonEncoder jsonEncoder = new HackedJsonEncoder(schema, output);
		GenericDatumWriter<GenericRecord> writer = 
				new GenericDatumWriter<GenericRecord>(schema);
		for (GenericRecord record : records) {
			writer.write(record, jsonEncoder);
		}
		jsonEncoder.flush();
		output.flush();
		return output.toString();
	}

	private static List<GenericRecord> toRecords(String inputJson,
			Schema writerSchema, Schema readerSchema) throws IOException {
		HackedJsonDecoder jsonDecoder = new HackedJsonDecoder(writerSchema,
				inputJson);
		GenericDatumReader<GenericRecord> reader = 
				new GenericDatumReader<GenericRecord>(
						writerSchema, readerSchema);
		List<GenericRecord> records = new ArrayList<GenericRecord>();
		while (true) {
			try {
				GenericRecord record = reader.read(null, jsonDecoder);
				records.add(record);
			} catch (EOFException e) {
				break;
			}
		}
		return records;
	}
}
