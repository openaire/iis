package eu.dnetlib.iis.core.examples.java;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Document;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.DocumentWithAuthors;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.PersonAge;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.PersonWithDocuments;
import eu.dnetlib.iis.core.java.PortBindings;
import eu.dnetlib.iis.core.java.Process;
import eu.dnetlib.iis.core.java.io.DataStore;
import eu.dnetlib.iis.core.java.io.FileSystemPath;
import eu.dnetlib.iis.core.java.porttype.AvroPortType;
import eu.dnetlib.iis.core.java.porttype.PortType;

/** Processes input files and produces files in a certain way
 *  that would require SQL JOINs when done in context of relational databases 
 * @author Mateusz Kobos
 */
public class Joiner implements Process {
	
	private final static String personPort = "person";
	private final static String documentPort = "document";
	private final static String documentWithAuthorsPort = 
			"document_with_authors";
	private final static String personWithDocumentsPort =
			"person_with_documents";
	private final static String personAgePort = "person_age";
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return createInputPorts();
	}
	
	@Override
	public Map<String, PortType> getOutputPorts() {
		return createOutputPorts();
	}

	private static HashMap<String, PortType> createInputPorts(){
		HashMap<String, PortType> inputPorts = 
				new HashMap<String, PortType>();
		inputPorts.put(personPort, 
				new AvroPortType(Person.SCHEMA$));
		inputPorts.put(documentPort, 
				new AvroPortType(Document.SCHEMA$));
		return inputPorts;
	}

	private static HashMap<String, PortType> createOutputPorts(){
		HashMap<String, PortType> outputPorts = 
				new HashMap<String, PortType>();
		outputPorts.put(documentWithAuthorsPort, 
				new AvroPortType(
						DocumentWithAuthors.SCHEMA$));
		outputPorts.put(personWithDocumentsPort, 
				new AvroPortType(
						PersonWithDocuments.SCHEMA$));
		outputPorts.put(personAgePort, 
				new AvroPortType(PersonAge.SCHEMA$));
		return outputPorts;	
	}
	
	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters)	throws IOException{
		Map<String, Path> input = portBindings.getInput();
		Map<String, Path> output = portBindings.getOutput();
		FileSystem fs = FileSystem.get(conf);
		
		List<Document> documents = DataStore.read(
				new FileSystemPath(fs, input.get(documentPort)));
		List<Person> persons = DataStore.read(
				new FileSystemPath(fs, input.get(personPort))); 
		
		List<DocumentWithAuthors> documentWithAuthors = 
				makeDocumentWithAuthors(documents, persons);
		List<PersonWithDocuments> personWithDocuments = 
				makePersonWithDocuments(documents, persons);
		List<PersonAge> personAge = makePersonAge(persons);

		DataStore.create(documentWithAuthors, 
				new FileSystemPath(fs, output.get(documentWithAuthorsPort)));
		DataStore.create(personWithDocuments, 
				new FileSystemPath(fs, output.get(personWithDocumentsPort)));
		DataStore.create(personAge, 
				new FileSystemPath(fs, output.get(personAgePort)));
	}
	
	private static List<DocumentWithAuthors> makeDocumentWithAuthors(
			List<Document> documents, List<Person> persons){
		List<DocumentWithAuthors> list = new ArrayList<DocumentWithAuthors>();
		for(Document d: documents){
			List<Person> authors = new ArrayList<Person>();
			for(int authorId: d.getAuthorIds()){
				Person author = getPerson(persons, authorId);
				authors.add(author);
			}
			list.add(DocumentWithAuthors.newBuilder()
					.setId(d.getId())
					.setTitle(d.getTitle())
					.setAuthors(authors)
					.build());
		}
		return list;
	}
	
	private static Person getPerson(List<Person> persons, int id){
		for(Person p: persons){
			if(p.getId() == id){
				return p;
			}
		}
		throw new RuntimeException(
				String.format("Person with id=%d not found", id));
	}
	
	private static List<PersonWithDocuments> makePersonWithDocuments(
			List<Document> documents, List<Person> persons){
		List<PersonWithDocuments> list = new ArrayList<PersonWithDocuments>();
		for(Person p: persons){
			List<eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.Document> personDocs = 
					new ArrayList<eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.Document>();
			for(Document d: documents){
				for(int authorId: d.getAuthorIds()){
					if(p.getId() == authorId){
						personDocs.add(eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.Document.newBuilder()
								.setId(d.getId())
								.setTitle(d.getTitle()).build());
						break;
					}
				}
			}
			list.add(PersonWithDocuments.newBuilder()
					.setPersonId(p.getId())
					.setDocuments(personDocs)
					.build());
		}
		return list;
	}
	
	private static List<PersonAge> makePersonAge(List<Person> persons){
		List<PersonAge> personAge = new ArrayList<PersonAge>();
		for(Person p: persons){
			personAge.add(PersonAge.newBuilder().setAge(p.getAge()).build());
		}
		return personAge;
	}
}
