package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

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
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.InferredDocumentProjectConverter;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.IisInferredDocumentProjectReader;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;

/**
 * @author mhorst
 */
@RunWith(MockitoJUnitRunner.class)
public class IisInferredDocumentProjectReaderTest {

    @InjectMocks
    private IisInferredDocumentProjectReader documentProjectReader = new IisInferredDocumentProjectReader();

    @Mock
    private SparkAvroLoader avroLoader;

    @Mock
    private InferredDocumentProjectConverter documentProjectConverter;

    @Mock
    private JavaSparkContext sparkContext;

    @Mock
    private JavaRDD<DocumentToProject> loadedDocumentProjects;

    @Captor
    private ArgumentCaptor<Function<DocumentToProject, AffMatchDocumentProject>> mapDocumentProjectFunction;

    @Mock
    private JavaRDD<AffMatchDocumentProject> documentProjects;

    private final String predefinedPath = "/path/to/document_pojects/";

    @Before
    public void setUp() {
        when(avroLoader.loadJavaRDD(sparkContext, predefinedPath, DocumentToProject.class)).thenReturn(loadedDocumentProjects);
        doReturn(documentProjects).when(loadedDocumentProjects).map(any());
    }

    // ------------------------ TESTS --------------------------

    @Test(expected = NullPointerException.class)
    public void readDocumentProjects_NULL_CONTEXT() {
        // execute
        documentProjectReader.readDocumentProjects(null, predefinedPath);
    }

    @Test(expected = NullPointerException.class)
    public void readDocumentProjects_NULL_PATH() {
        // execute
        documentProjectReader.readDocumentProjects(sparkContext, null);
    }

    @Test
    public void readDocumentProjects() throws Exception {
        // execute
        JavaRDD<AffMatchDocumentProject> retDocumentProject = documentProjectReader.readDocumentProjects(sparkContext,
                predefinedPath);
        // assert
        assertTrue(retDocumentProject == documentProjects);
        verify(avroLoader).loadJavaRDD(sparkContext, predefinedPath, DocumentToProject.class);
        verify(loadedDocumentProjects).map(mapDocumentProjectFunction.capture());
        assertMapDocumentProjectFunction(mapDocumentProjectFunction.getValue());
    }

    // ------------------------ PRIVATE --------------------------

    private void assertMapDocumentProjectFunction(Function<DocumentToProject, AffMatchDocumentProject> function)
            throws Exception {
        // given
        DocumentToProject documentProject = mock(DocumentToProject.class);
        AffMatchDocumentProject mappedDocumentProject = mock(AffMatchDocumentProject.class);
        when(documentProjectConverter.convert(documentProject)).thenReturn(mappedDocumentProject);
        // execute
        AffMatchDocumentProject retDocumentProject = function.call(documentProject);
        // assert
        assertTrue(retDocumentProject == mappedDocumentProject);
    }

}
