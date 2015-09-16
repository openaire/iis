package eu.dnetlib.iis.core.examples.pig;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractWorkflowTestCase;
import eu.dnetlib.iis.core.RemoteOozieAppManager;
import eu.dnetlib.iis.core.TestsIOUtils;
import eu.dnetlib.iis.core.examples.StandardDataStoreExamples;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.DocumentWithAuthors;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.PersonAge;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.Document;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.PersonWithDocuments;
import java.io.File;
import java.util.*;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * 
 * @author Mateusz Kobos
 * @author Dominika Tkaczyk
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractWorkflowTestCase {

	@Test
	public void testOnSimpleCSVFiles() throws Exception{
		RemoteOozieAppManager appManager = 
				runWorkflow("eu/dnetlib/iis/core/examples/pig/basic/oozie_app");
		
		File localDir = new File(getTestCaseDir());
		final File actualPersonId = 
				new File(localDir, "person_id.csv");
		@SuppressWarnings("serial")
		Map<String, File> hdfsFileNameToLocalFileNameMap = 
				new HashMap<String, File>(){{
					put("pig_node/person_id/part-m-00000", actualPersonId);
				}};			
		appManager.copyFilesFromWorkingDir(hdfsFileNameToLocalFileNameMap);
		
		TestsIOUtils.assertContentsEqual(
				"eu/dnetlib/iis/core/examples/simple_csv_data/person_id.csv", 
				actualPersonId);
	}

    @Test
	public void testJoin() throws Exception {
		RemoteOozieAppManager appManager = 
				runWorkflow("eu/dnetlib/iis/core/examples/pig/joiner/oozie_app");
        
        List <DocumentWithAuthors> documentWithAuthors = 
            appManager.readDataStoreFromWorkingDir("joiner/document_with_authors");
        List <PersonWithDocuments> personWithDocuments = 
			appManager.readDataStoreFromWorkingDir("joiner/person_with_documents");
        List <PersonAge> personAge = 
			appManager.readDataStoreFromWorkingDir("joiner/person_age");
    
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
	public void testJoinWithExplicitSchema() throws Exception {
		RemoteOozieAppManager appManager = 
				runWorkflow("eu/dnetlib/iis/core/examples/pig/joiner_with_explicit_schema/oozie_app");
        
        List <DocumentWithAuthors> documentWithAuthors = 
            appManager.readDataStoreFromWorkingDir("joiner/document_with_authors");
        List <PersonWithDocuments> personWithDocuments = 
			appManager.readDataStoreFromWorkingDir("joiner/person_with_documents");
        List <PersonAge> personAge = 
			appManager.readDataStoreFromWorkingDir("joiner/person_age");
    
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
		RemoteOozieAppManager appManager = 
				runWorkflow("eu/dnetlib/iis/core/examples/pig/person_by_docs_filter/oozie_app");
        
        List<PersonWithDocuments> testPersonWithDocuments = 
			appManager.readDataStoreFromWorkingDir("filter/person_with_documents");

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
		RemoteOozieAppManager appManager = 
				runWorkflow("eu/dnetlib/iis/core/examples/pig/person_by_docs_filter_with_subworkflow/oozie_app");
        
        List<PersonWithDocuments> testPersonWithDocuments = 
			appManager.readDataStoreFromWorkingDir("my_subworkflow/person_with_documents");
       
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
