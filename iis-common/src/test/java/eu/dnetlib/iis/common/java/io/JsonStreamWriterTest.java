package eu.dnetlib.iis.common.java.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import eu.dnetlib.iis.common.StaticResourceProvider;
import org.junit.Assert;
import org.junit.Test;

import eu.dnetlib.iis.common.avro.Document;

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
		String expected = StaticResourceProvider
				.getResourceContent("eu/dnetlib/iis/common/java/io/document.json")
				.replace("\r", "");
		Assert.assertEquals(expected, actual);
	}
}
