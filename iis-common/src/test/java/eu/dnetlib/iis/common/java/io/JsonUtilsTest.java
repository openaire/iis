package eu.dnetlib.iis.common.java.io;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

import eu.dnetlib.iis.common.TestsIOUtils;
import eu.dnetlib.iis.common.avro.Document;

/**
 * 
 * @author Mateusz Kobos
 *
 */
public class JsonUtilsTest {
	private File tempDir = null;
	
	@Before
	public void setUp() throws IOException{
		tempDir = Files.createTempDir();
	}
	
	@After
	public void tearDown() throws IOException{
		FileUtils.deleteDirectory(tempDir);
	}
	
	@Test
	public void testConvertToDataStoreSimple() throws IOException{
		checkConvertToDataStore(DataStoreExamples.getDocument(), 
				"eu/dnetlib/iis/common/java/io/document.json", Document.SCHEMA$);
	}
	
	@Test
	public void testConvertToDataStoreWithEscapeCodes() throws IOException{
		checkConvertToDataStore(DataStoreExamples.getDocumentWithUnicodeEscapeCodes(), 
				"eu/dnetlib/iis/common/java/io/document_with_unicode_escape_codes.json", 
				Document.SCHEMA$);
	}
	
	private <T> void checkConvertToDataStore(List<T> expectedRecords,
			String actualResourcePath, Schema actualSchema) throws IOException{
		InputStream in = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream(actualResourcePath);
		FileSystemPath outPath = new FileSystemPath(new File(tempDir, "record"));
		JsonUtils.convertToDataStore(actualSchema, in, outPath);
		TestsIOUtils.assertEqualSets(expectedRecords, 
				new AvroDataStoreReader<T>(outPath));
	}
	
	@Test
	public void testConvertToListWithUnicodeEscapeCodes() throws IOException{
		List<Document> actual = JsonUtils.convertToList(
				"eu/dnetlib/iis/common/java/io/document_with_unicode_escape_codes.json",
				Document.SCHEMA$, Document.class);
		List<Document> expected = DataStoreExamples.getDocumentWithUnicodeEscapeCodes();
		TestsIOUtils.assertEqualSets(expected, actual);			
	}
	
	@Test
	public void testPrettyPrint() throws IOException{
		String uglyJson = getStringFromResourceFile(
				"eu/dnetlib/iis/common/java/io/json_pretty_print/ugly_json.json");
		String actual = JsonUtils.toPrettyJSON(uglyJson);
		String expected = getStringFromResourceFile(
				"eu/dnetlib/iis/common/java/io/json_pretty_print/expected_pretty_json.json");
		Assert.assertEquals(expected, actual);
	}
	
	private String getStringFromResourceFile(String resourcePath) throws IOException{
		InputStream in = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream(resourcePath);
		return IOUtils.toString(in, "UTF-8").replaceAll("\\r", "");
	}
}
