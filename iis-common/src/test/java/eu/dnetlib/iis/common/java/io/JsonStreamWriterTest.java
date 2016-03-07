package eu.dnetlib.iis.common.java.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.util.List;

import org.apache.commons.io.IOUtils;
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
		Reader in = new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("eu/dnetlib/iis/common/java/io/document.json"), "UTF-8");
		String expected = toString(in);
		Assert.assertEquals(expected, actual);
	}
	
	private static String toString(Reader in) throws IOException{
		StringWriter writer = new StringWriter();
		IOUtils.copy(in, writer);
		return writer.toString().replace("\r", "");
	}
}
