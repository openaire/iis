package eu.dnetlib.iis.common.java.io;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

import eu.dnetlib.iis.common.TestsIOUtils;
import eu.dnetlib.iis.common.avro.Document;
import eu.dnetlib.iis.common.avro.DocumentWithoutTitle;


/**
 * @author Mateusz Kobos
 */
public class DataStoreTest {

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
	public void testSingleFile() throws IOException {
		List<Document> documents = DataStoreExamples.getDocument();
		FileSystemPath path = new FileSystemPath(new File(tempDir, "doc"));
		DataStore.create(documents, path);
		TestsIOUtils.assertEqualSets(documents, 
				new AvroDataStoreReader<Document>(path));	
	}
	
	@Test
	public void testReaderSchema() throws IOException {
		List<Document> documents = DataStoreExamples.getDocument();
		FileSystemPath path = new FileSystemPath(new File(tempDir, "doc"));
		DataStore.create(documents, path);
		List<DocumentWithoutTitle> documentsWithoutTitle = 
				DataStoreExamples.getDocumentWithoutTitle();
		TestsIOUtils.assertEqualSets(documentsWithoutTitle, 
				new AvroDataStoreReader<DocumentWithoutTitle>(
						path, DocumentWithoutTitle.SCHEMA$));	
	}
	
	@Test
	public void testClose() throws IOException{
		List<Document> documents = DataStoreExamples.getDocument();
		FileSystemPath path = new FileSystemPath(new File(tempDir, "doc"));
		DataStore.create(documents, path);
		AvroDataStoreReader<Document> reader = 
				new AvroDataStoreReader<Document>(path);
		Assert.assertEquals(documents.get(0), reader.next());
		Assert.assertEquals(documents.get(1), reader.next());
		reader.close();
		try{
			reader.next();
			Assert.fail("Didn't throw an exception when it supposed to");
		} catch(NoSuchElementException ex){
		}
	}
	
	@Test
	public void testManyFiles() throws IOException{
		List<Document> documents = DataStoreExamples.getDocument();
		createSingleFile(documents.subList(0, 3), 
				new FileSystemPath(new File(tempDir, "doc0")), 
				Document.SCHEMA$);
		createSingleFile(documents.subList(3, 4), 
				new FileSystemPath(new File(tempDir, "doc1")),
				Document.SCHEMA$);
	
		TestsIOUtils.assertEqualSets(documents, 
				new AvroDataStoreReader<Document>(
						new FileSystemPath(tempDir)));	
	}
	
	@Test
	public void testEmptyFiles() throws IOException{
		List<Document> documents = DataStoreExamples.getDocument();

		createSingleFile(documents.subList(0, 3), 
				new FileSystemPath(new File(tempDir, "doc0")),
				Document.SCHEMA$);
		
		createSingleFile(Arrays.<Document>asList(), 
				new FileSystemPath(new File(tempDir, "doc881")),
				Document.SCHEMA$);
		
		createSingleFile(documents.subList(3, 4), 
				new FileSystemPath(new File(tempDir, "doc22")),
				Document.SCHEMA$);

		createSingleFile(Arrays.<Document>asList(), 
				new FileSystemPath(new File(tempDir, "doc4")),
				Document.SCHEMA$);
		
		TestsIOUtils.assertEqualSets(documents, 
				new AvroDataStoreReader<Document>(
						new FileSystemPath(tempDir)));	
	}
	
	private static <T extends GenericContainer> void createSingleFile(
			List<T> elements, FileSystemPath path, Schema schema) 
					throws IOException{
		DataFileWriter<T> writer = DataStore.createSingleFile(path, schema);
		for(T i: elements){
			writer.append(i);
		}
		writer.close();
	}
}

class DataStoreExamples{
	static String bookTitleExtra = "An extraordinary book";
	static String bookTitleBasics = "Basics of the basics";
	static String bookTitleMoreInteresting = "Even more of interesting stuff";
	static String bookTitleEscapeCodes = "2.2. Stellar Rotation Hertszprung\u2013Russell diagram precludes detecting\n6.2. Conclusions\n";
		
	/**
	 * Sample data on some documents
	 */
	public static List<Document> getDocument(){
		ArrayList<Document> list = new ArrayList<Document>();
		list.add(new Document(20, bookTitleExtra,
				new ArrayList<Integer>(Arrays.asList(1, 20))));
		list.add(new Document(1, bookTitleBasics,
				new ArrayList<Integer>(Arrays.asList(10, 6, 1))));
		list.add(new Document(2, null, 
				new ArrayList<Integer>()));
		list.add(new Document(6, bookTitleMoreInteresting,
				new ArrayList<Integer>(Arrays.asList(1, 6))));
		return list;
	}
	
	/**
	 * The same as the {@link getDocument} but does not include document title
	 */
	public static List<DocumentWithoutTitle> getDocumentWithoutTitle(){
		ArrayList<DocumentWithoutTitle> list = new ArrayList<DocumentWithoutTitle>();
		list.add(new DocumentWithoutTitle(20, 
				new ArrayList<Integer>(Arrays.asList(1, 20))));
		list.add(new DocumentWithoutTitle(1, 
				new ArrayList<Integer>(Arrays.asList(10, 6, 1))));
		list.add(new DocumentWithoutTitle(2,
				new ArrayList<Integer>()));
		list.add(new DocumentWithoutTitle(6, 
				new ArrayList<Integer>(Arrays.asList(1, 6))));
		return list;
	}
	
	public static List<Document> getDocumentWithUnicodeEscapeCodes(){
		ArrayList<Document> list = new ArrayList<Document>();
		list.add(new Document(20, bookTitleEscapeCodes,
				new ArrayList<Integer>(Arrays.asList(1, 20))));
		return list;
	}
}
