package eu.dnetlib.iis.common.java.io;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.avro.Document;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Mateusz Kobos
 */
public class JsonStreamWriterTest {
	
	@Test
	public void basicTest() throws IOException{
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		JsonStreamWriter<Document> writer = 
				new JsonStreamWriter<Document>(Document.SCHEMA$, out);
		List<Document> documents = DataStoreExamples.getDocument();
		for(Document d: documents){
			writer.write(d);
		}
		writer.close();
		String actual = out.toString();
		String expected = ClassPathResourceProvider
				.getResourceContent("eu/dnetlib/iis/common/java/io/document.json")
				.replace("\r", "");
		assertEquals(expected, actual);
	}
}
