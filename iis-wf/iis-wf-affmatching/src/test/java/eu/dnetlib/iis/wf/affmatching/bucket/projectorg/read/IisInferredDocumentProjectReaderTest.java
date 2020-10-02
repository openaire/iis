package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

/**
 * @author mhorst
 */
@ExtendWith(MockitoExtension.class)
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

    @BeforeEach
    public void setUp() {
        lenient().when(avroLoader.loadJavaRDD(sparkContext, predefinedPath, DocumentToProject.class)).thenReturn(loadedDocumentProjects);
        lenient().doReturn(documentProjects).when(loadedDocumentProjects).map(any());
    }

    // ------------------------ TESTS --------------------------

    @Test
    public void readDocumentProjects_NULL_CONTEXT() {
        // execute
        assertThrows(NullPointerException.class, () -> documentProjectReader.readDocumentProjects(null, predefinedPath));
    }

    @Test
    public void readDocumentProjects_NULL_PATH() {
        // execute
        assertThrows(NullPointerException.class, () -> documentProjectReader.readDocumentProjects(sparkContext, null));
    }

    @Test
    public void readDocumentProjects() throws Exception {
        // execute
        JavaRDD<AffMatchDocumentProject> retDocumentProject = documentProjectReader.readDocumentProjects(sparkContext,
                predefinedPath);
        // assert
        assertSame(retDocumentProject, documentProjects);
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
        assertSame(retDocumentProject, mappedDocumentProject);
    }

}
