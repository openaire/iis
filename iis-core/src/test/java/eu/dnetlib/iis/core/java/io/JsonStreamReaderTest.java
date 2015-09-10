package eu.dnetlib.iis.core.java.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import eu.dnetlib.iis.core.TestsIOUtils;
import eu.dnetlib.iis.core.schemas.standardexamples.Document;

/**
 * @author Mateusz Kobos
 */
public class JsonStreamReaderTest {
	
	@Test
	public void basicTest() throws IOException{
		InputStream in = Thread.currentThread().getContextClassLoader()
			.getResourceAsStream("eu/dnetlib/iis/core/java/io/document.json");
		CloseableIterator<Document> reader = new JsonStreamReader<Document>(
				Document.SCHEMA$, in, Document.class);
		List<Document> expected = DataStoreExamples.getDocument();
		List<Document> actual = new ArrayList<Document>();
		while(reader.hasNext()){
			Object record = reader.next();
			Document read = (Document) record;
			actual.add(read);
		}
		TestsIOUtils.assertEqualSets(expected, actual);	
	}
}
