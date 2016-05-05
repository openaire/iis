package eu.dnetlib.iis.wf.affmatching.read;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.model.DocumentProject;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;

/**
 * @author mhorst
 */
@RunWith(MockitoJUnitRunner.class)
public class IisDocumentProjectReaderTest {

    @InjectMocks
    private IisDocumentProjectReader documentProjectReader = new IisDocumentProjectReader();

    @Mock
    private SparkAvroLoader avroLoader;

    @Mock
    private DocumentProjectConverter documentProjectConverter;

    @Mock
    private JavaSparkContext sparkContext;

    @Mock
    private JavaRDD<DocumentToProject> loadedDocumentProjects;

    @Captor
    private ArgumentCaptor<Function<DocumentToProject, DocumentProject>> mapDocumentProjectFunction;

    @Mock
    private JavaRDD<DocumentProject> documentProjects;

    private final String predefinedPath = "/path/to/document_pojects/";

    @Before
    public void setUp() {
        doReturn(loadedDocumentProjects).when(avroLoader).loadJavaRDD(sparkContext, predefinedPath,
                DocumentToProject.class);
        doReturn(documentProjects).when(loadedDocumentProjects).map(any());
    }

    // ------------------------ TESTS --------------------------

    @Test(expected = NullPointerException.class)
    public void readDocumentProject_NULL_CONTEXT() {
        // execute
        documentProjectReader.readDocumentProject(null, predefinedPath);
    }

    @Test(expected = NullPointerException.class)
    public void readDocumentProject_NULL_PATH() {
        // execute
        documentProjectReader.readDocumentProject(sparkContext, null);
    }

    @Test
    public void readDocumentProject() throws Exception {
        // execute
        JavaRDD<DocumentProject> retDocumentProject = documentProjectReader.readDocumentProject(sparkContext,
                predefinedPath);
        // assert
        assertTrue(retDocumentProject == documentProjects);
        verify(avroLoader).loadJavaRDD(sparkContext, predefinedPath, DocumentToProject.class);
        verify(loadedDocumentProjects).map(mapDocumentProjectFunction.capture());
        assertMapDocumentProjectFunction(mapDocumentProjectFunction.getValue());
    }

    // ------------------------ PRIVATE --------------------------

    private void assertMapDocumentProjectFunction(Function<DocumentToProject, DocumentProject> function)
            throws Exception {
        DocumentToProject documentProject = mock(DocumentToProject.class);
        DocumentProject mappedDocumentProject = mock(DocumentProject.class);
        when(documentProjectConverter.convert(documentProject)).thenReturn(mappedDocumentProject);
        DocumentProject retDocumentProject = function.call(documentProject);
        assertTrue(retDocumentProject == mappedDocumentProject);
    }

}
