package eu.dnetlib.iis.common.java.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import eu.dnetlib.iis.common.StaticResourceProvider;
import org.junit.Test;

import eu.dnetlib.iis.common.TestsIOUtils;
import eu.dnetlib.iis.common.avro.Document;

/**
 * @author Mateusz Kobos
 */
public class JsonStreamReaderTest {
	
	@Test
	public void basicTest() throws IOException{
		InputStream in = StaticResourceProvider
				.getResourceInputStream("eu/dnetlib/iis/common/java/io/document.json");
		CloseableIterator<Document> reader = new JsonStreamReader<Document>(
				Document.SCHEMA$, in, Document.class);
		List<Document> expected = DataStoreExamples.getDocument();
		List<Document> actual = new ArrayList<Document>();
		while(reader.hasNext()){
			Object record = reader.next();
			Document read = (Document) record;
			actual.add(read);
		}
		reader.close();
		TestsIOUtils.assertEqualSets(expected, actual);	
	}
}
