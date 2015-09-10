package eu.dnetlib.iis.core.java.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import junit.framework.Assert;

import eu.dnetlib.iis.core.schemas.standardexamples.Document;

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
		InputStream in = Thread.currentThread().getContextClassLoader()
			.getResourceAsStream("eu/dnetlib/iis/core/java/io/document.json");
		String expected = toString(in);
		Assert.assertEquals(expected, actual);
	}
	
	private static String toString(InputStream in) throws IOException{
		StringWriter writer = new StringWriter();
		IOUtils.copy(in, writer);
		return writer.toString();
	}
}
