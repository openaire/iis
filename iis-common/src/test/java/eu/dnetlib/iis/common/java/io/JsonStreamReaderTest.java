package eu.dnetlib.iis.common.java.io;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.TestsIOUtils;
import eu.dnetlib.iis.common.avro.Document;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Mateusz Kobos
 */
public class JsonStreamReaderTest {
	
	@Test
	public void basicTest() throws IOException{
		InputStream in = ClassPathResourceProvider
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
