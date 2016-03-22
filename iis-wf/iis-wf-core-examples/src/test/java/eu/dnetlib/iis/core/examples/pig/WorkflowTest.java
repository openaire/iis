package eu.dnetlib.iis.core.examples.pig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import eu.dnetlib.iis.common.TestsIOUtils;
import eu.dnetlib.iis.common.WorkflowTestResult;
import eu.dnetlib.iis.core.examples.StandardDataStoreExamples;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.DocumentWithAuthors;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.PersonAge;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.Document;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.PersonWithDocuments;

/**
 * 
 * @author Mateusz Kobos
 * @author Dominika Tkaczyk
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

	@Test
	public void testOnSimpleCSVFiles() throws Exception{
		OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
		conf.addExpectedOutputFile("pig_node/person_id/part-m-00000");
		
		WorkflowTestResult workflowTestResult = testWorkflow("eu/dnetlib/iis/core/examples/pig/basic", conf);
		
		TestsIOUtils.assertContentsEqual(
				"eu/dnetlib/iis/core/examples/simple_csv_data/person_id.csv", 
				workflowTestResult.getWorkflowOutputFile("pig_node/person_id/part-m-00000"));
	}

	@Test
	public void testJoin() throws Exception {
		OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
		conf.addExpectedOutputAvroDataStore("joiner/document_with_authors");
		conf.addExpectedOutputAvroDataStore("joiner/person_with_documents");
		conf.addExpectedOutputAvroDataStore("joiner/person_age");

		WorkflowTestResult workflowTestResult = 
				testWorkflow("eu/dnetlib/iis/core/examples/pig/joiner", conf);

		List<DocumentWithAuthors> documentWithAuthors = workflowTestResult.getAvroDataStore("joiner/document_with_authors");
		List<PersonWithDocuments> personWithDocuments = workflowTestResult.getAvroDataStore("joiner/person_with_documents");
		List<PersonAge> personAge = workflowTestResult.getAvroDataStore("joiner/person_age");

		/*
		 * DocumentWithAuthors and PersonWithDocuments objects include nested 
		 * lists of elements (authors and documents lists, respectively). 
		 * The order of the elements in the lists does not matter and should 
		 * not affect the sets comparison. To achieve this, before we can 
		 * compare the sets, the nested authors and documents lists need to be 
		 * sorted.
		 */
		TestsIOUtils.assertEqualSets(
				sortAuthorLists(StandardDataStoreExamples.getDocumentWithAuthors()),
				sortAuthorLists(documentWithAuthors));
		TestsIOUtils.assertEqualSets(
				sortDocLists(StandardDataStoreExamples.getPersonWithDocuments()),
				sortDocLists(personWithDocuments));
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonAge(),
				personAge);
	}

	@Test
	public void testPersonFilteredByDocumentsNumber() throws Exception {
		OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
		conf.addExpectedOutputAvroDataStore("filter/person_with_documents");

		WorkflowTestResult workflowTestResult = 
				testWorkflow("eu/dnetlib/iis/core/examples/pig/person_by_docs_filter", conf);

		List<PersonWithDocuments> testPersonWithDocuments = 
				workflowTestResult.getAvroDataStore("filter/person_with_documents");

		List<PersonWithDocuments> expPersonWithDocuments = 
				new ArrayList<PersonWithDocuments>();
		for (PersonWithDocuments person : StandardDataStoreExamples.getPersonWithDocuments()) {
			if (person.getDocuments().size() >= 2) {
				expPersonWithDocuments.add(person);
			}
		}

		TestsIOUtils.assertEqualSets(
				sortDocLists(expPersonWithDocuments),
				sortDocLists(testPersonWithDocuments));
	}

	@Test
	public void testPersonFilteredByDocumentsNumberWithSubworkflow() throws Exception {
		OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
		conf.addExpectedOutputAvroDataStore("my_subworkflow/person_with_documents");

		WorkflowTestResult workflowTestResult = 
				testWorkflow("eu/dnetlib/iis/core/examples/pig/person_by_docs_filter_with_subworkflow", conf);

		List<PersonWithDocuments> testPersonWithDocuments = 
				workflowTestResult.getAvroDataStore("my_subworkflow/person_with_documents");

		List<PersonWithDocuments> expPersonWithDocuments =
				new ArrayList<PersonWithDocuments>();
		for (PersonWithDocuments person : StandardDataStoreExamples.getPersonWithDocuments()) {
			if (person.getDocuments().size() >= 2) {
				expPersonWithDocuments.add(person);
			}
		}

		TestsIOUtils.assertEqualSets(
				sortDocLists(expPersonWithDocuments),
				sortDocLists(testPersonWithDocuments));
	}
    
    /**
     * The method sort authors lists contained by DocumentWithAuthors objects.
     * The sorting is based on person id.
     * 
     * @param docWithAuthorsList document with authors list
     * @return the list with sorted authors' lists
     */
    private List<DocumentWithAuthors> sortAuthorLists(
            List<DocumentWithAuthors> docWithAuthorsList) {
        for (DocumentWithAuthors docWithAuthors : docWithAuthorsList) {
            Collections.sort(docWithAuthors.getAuthors(), new Comparator<Person>() {

                @Override
                public int compare(Person t1, Person t2) {
                    return t1.getId().compareTo(t2.getId());
                }
            });
        }
        return docWithAuthorsList;
    }
    
    /**
     * The method sorts documents lists contained by PersonWithDocuments objects.
     * The sorting is based on document id.
     * 
     * @param personWithDocsList person with documents list
     * @return the list with sorted documents' lists
     */
    private List<PersonWithDocuments> sortDocLists(
            List<PersonWithDocuments> personWithDocsList) {
        for (PersonWithDocuments personWithDocs : personWithDocsList) {
            Collections.sort(personWithDocs.getDocuments(), new Comparator<Document>() {

                @Override
                public int compare(Document t1, Document t2) {
                    return t1.getId().compareTo(t2.getId());
                }
            });
        }
        return personWithDocsList;
    }
    
}
