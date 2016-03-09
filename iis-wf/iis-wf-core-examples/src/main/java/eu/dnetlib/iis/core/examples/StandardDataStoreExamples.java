package eu.dnetlib.iis.core.examples;

import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.io.JsonUtils;
import eu.dnetlib.iis.core.examples.schemas.WordCount;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Document;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.DocumentWithAuthors;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.PersonAge;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.PersonWithDocuments;
import eu.dnetlib.iis.core.examples.schemas.documenttext.DocumentText;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

/**
 * @author Mateusz Kobos
 */
public class StandardDataStoreExamples {
	
	private static String bookTitleExtra = "An extraordinary book";
	private static String bookTitleBasics = "Basics of the basics";
	private static String bookTitleInteresting = null;
	private static String bookTitleMoreInteresting = "Even more of interesting stuff";
	private static String personDrWho = "Dr. Who";
	
	
	/**
	 * Sample data on some documents
	 * @throws IOException 
	 */
	public static List<Document> getDocument(){
		return JsonUtils.convertToList(
				"eu/dnetlib/iis/core/examples/data/document.json",
				Document.SCHEMA$, Document.class);
	}
	
	/**
	 * Sample data on some persons
	 */
	public static List<Person> getPerson(){
		return getPersonRepeated(1);
	}

	/**
	 * Similar to {@link getPerson()}, but the records are repeated a 
	 * couple of times.
	 * @param copies number of times the records are repeated
	 * @throws IOException 
	 */
	public static List<Person> getPersonRepeated(int copies){
		ArrayList<Person> list = new ArrayList<Person>();
		for(int i = 0; i < copies; i++){
			List<Person> singleList = 
				JsonUtils.convertToList(
						"eu/dnetlib/iis/core/examples/data/person.json",
						Person.SCHEMA$, Person.class);
			list.addAll(singleList);
		}
		return list;
	}
	
	/**
	 * Gets person age objects.
	 * @param copies number of times the records are repeated
	 */
	public static List<PersonAge> getPersonAgeRepeated(int copies){
		List<Person> persons = getPersonRepeated(copies);
		ArrayList<PersonAge> personsAges = new ArrayList<PersonAge>(copies);
		for (Person currentPerson : persons) {
			personsAges.add(new PersonAge(currentPerson.getAge()));
		}
		return personsAges;
	}
	
	/**
	 * @return age of each person contained in the output of the
	 *  {@link getPerson()} 
	 */
	public static List<PersonAge> getPersonAge(){
		ArrayList<PersonAge> list = 
				new ArrayList<PersonAge>();
		int[] age = new int[]{2349, 46, 40, 50, 12};
		for(int i = 0; i < age.length; i++){
			list.add(new PersonAge(age[i]));
		}
		return list;
	}
	
	/**
	 * @return information about documents and their authors. This data comes 
	 * from joining the data returned by 
	 * {@link getPerson()} and {@link getDocument()} 
	 */
	public static List<DocumentWithAuthors> getDocumentWithAuthors(){
		ArrayList<DocumentWithAuthors> list = 
				new ArrayList<DocumentWithAuthors>();
		list.add(DocumentWithAuthors.newBuilder()
				.setId(20)
				.setTitle(bookTitleExtra)
				.setAuthors(Arrays.asList(
						Person.newBuilder()
						.setId(1)
						.setName(personDrWho)
						.setAge(2349)
						.build(),
						Person.newBuilder()
						.setId(20)
						.setName("Stieg Larsson")
						.setAge(50)
						.build()))
				.build());
		list.add(DocumentWithAuthors.newBuilder()
				.setId(1)
				.setTitle(bookTitleBasics)
				.setAuthors(Arrays.asList(
						Person.newBuilder()
						.setId(10)
						.setName("Mikael Blomkvist")
						.setAge(46)
						.build(),
						Person.newBuilder()
						.setId(6)
						.setName("Lisbeth Salander")
						.setAge(12)
						.build(),
						Person.newBuilder()
						.setId(1)
						.setName(personDrWho)
						.setAge(2349)
						.build()))
				.build());
		list.add(DocumentWithAuthors.newBuilder()
				.setId(2)
				.setTitle(bookTitleInteresting)
				.setAuthors(new ArrayList<Person>())
				.build());
		list.add(DocumentWithAuthors.newBuilder()
				.setId(6)
				.setTitle(bookTitleMoreInteresting)
				.setAuthors(Arrays.asList(
						Person.newBuilder()
						.setId(1)
						.setName(personDrWho)
						.setAge(2349)
						.build(),
						Person.newBuilder()
						.setId(6)
						.setName("Lisbeth Salander")
						.setAge(12)
						.build()))
				.build());
		return list;
	}

	/**
	 * @return information about person and documents authored by them. 
	 * The persons that haven't authored any books are still present in this 
	 * list. This data comes from joining the data returned by 
	 * {@link getPerson()} and {@link getDocument()} 
	 */
	public static List<PersonWithDocuments> getPersonWithDocuments(){
		List<PersonWithDocuments> list = 
				getPersonWithDocumentsWithoutDocumentlessPersons();
		list.add(PersonWithDocuments.newBuilder()
				.setPersonId(5)
				.setDocuments(new ArrayList<eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.Document>())
				.build());
		return list;
	}
	
	/**
	 * @return information about person and documents authored by them. 
	 * The persons that haven't authored any books are <b>not present</b> 
	 * in this list. This data comes from joining the data returned by 
	 * {@link getPerson()} and {@link getDocument()} 
	 */
	public static List<PersonWithDocuments> getPersonWithDocumentsWithoutDocumentlessPersons(){
		ArrayList<PersonWithDocuments> list = 
				new ArrayList<PersonWithDocuments>();
		list.add(PersonWithDocuments.newBuilder()
				.setPersonId(1)
				.setDocuments(Arrays.asList(
						eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.Document.newBuilder()
						.setId(20)
						.setTitle(bookTitleExtra)
						.build(),
						eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.Document.newBuilder()
						.setId(1)
						.setTitle(bookTitleBasics)
						.build(),
						eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.Document.newBuilder()
						.setId(6)
						.setTitle(bookTitleMoreInteresting)
						.build()))
				.build());
		list.add(PersonWithDocuments.newBuilder()
				.setPersonId(10)
				.setDocuments(Arrays.asList(
						eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.Document.newBuilder()
						.setId(1)
						.setTitle(bookTitleBasics)
						.build()))
				.build());
		list.add(PersonWithDocuments.newBuilder()
				.setPersonId(20)
				.setDocuments(Arrays.asList(
						eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.Document.newBuilder()
						.setId(20)
						.setTitle(bookTitleExtra)
						.build()))
				.build());
		list.add(PersonWithDocuments.newBuilder()
				.setPersonId(6)
				.setDocuments(Arrays.asList(
						eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.Document.newBuilder()
						.setId(1)
						.setTitle(bookTitleBasics)
						.build(),
						eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.Document.newBuilder()
						.setId(6)
						.setTitle(bookTitleMoreInteresting)
						.build()))
				.build());
		return list;
	}
	
    public static List<WordCount> getWordCountWithoutStopwords(){
		return JsonUtils.convertToList(
				"eu/dnetlib/iis/core/examples/data/wordcount_without_stopwords.json",
				WordCount.SCHEMA$, WordCount.class);
	}
    
	public static List<DocumentText> getDocumentTextRepeated(int copies){
		ArrayList<DocumentText> list = new ArrayList<DocumentText>();
		for(int i = 0; i < copies; i++){
			List<DocumentText> singleList = 
				JsonUtils.convertToList(
						"eu/dnetlib/iis/core/examples/hadoopstreaming/cloner_with_unicode_escape_codes/producer_data/documents.json",
						DocumentText.SCHEMA$, DocumentText.class);
			list.addAll(singleList);
		}
		return list;
	}
	
	/**
	 * Write some of the example data stores to the current directory.
	 * 
	 * This is a code for generating data for some manual tests.
	 */
	public static void main(String[] args) throws IOException, URISyntaxException{
		FileSystem fs = new RawLocalFileSystem();
		fs.initialize(new URI("file:///"), new Configuration());
		FileSystemPath path = new FileSystemPath(fs, new Path("document"));
		DataStore.create(getDocument(), path);
		List<Document> elements = DataStore.read(path);
		for(Document e: elements){
			System.out.println("\ndocument data:\n==============");
			System.out.println(e.toString());
		}
		DataStore.create(getPerson(),
				new FileSystemPath(fs, new Path("person")));
		
	}
}
