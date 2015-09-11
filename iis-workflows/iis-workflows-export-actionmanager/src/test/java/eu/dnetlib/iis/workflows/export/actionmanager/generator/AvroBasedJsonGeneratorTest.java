package eu.dnetlib.iis.workflows.export.actionmanager.generator;

import java.io.StringWriter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.junit.Test;

import eu.dnetlib.iis.documentssimilarity.schemas.DocumentSimilarity;

/**
 * Test class for verifying json generation from avro object and vice versa.
 * @author mhorst
 *
 */
public class AvroBasedJsonGeneratorTest {

	@Test
	public void testSimilarity() throws Exception {
		String docId = "0314fe20-be3c-4bc3-adee-6bbc2cde3cb7_UmVwb3NpdG9yeVNlcnZpY2VSZXNvdXJjZXMvUmVwb3NpdG9yeVNlcnZpY2VSZXNvdXJjZVR5cGU=::oai:DiVA.org:uu-127423";
		String otherDocId = "022aab67-9851-42a1-ad4c-0d8df95f44e7_UmVwb3NpdG9yeVNlcnZpY2VSZXNvdXJjZXMvUmVwb3NpdG9yeVNlcnZpY2VSZXNvdXJjZVR5cGU=::oai:rabida.uhu.es:10272/5383";
		DocumentSimilarity.Builder simBuilder = DocumentSimilarity.newBuilder();
		simBuilder.setDocumentId(docId);
		simBuilder.setOtherDocumentId(otherDocId);
		simBuilder.setSimilarity(0.9f);
		
		EncoderFactory encoderFactory = new EncoderFactory();
		Schema schema = DocumentSimilarity.SCHEMA$;
		DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);
		StringWriter strWriter = new StringWriter();
		JsonGenerator g = new JsonFactory().createJsonGenerator(strWriter);
		g.useDefaultPrettyPrinter();
		writer.write(simBuilder.build(), encoderFactory.jsonEncoder(schema, g));
		g.flush();
		g.close();
		strWriter.flush();
		String jsonStr = strWriter.toString();
		strWriter.close();
		System.out.println(jsonStr);
	}

}
