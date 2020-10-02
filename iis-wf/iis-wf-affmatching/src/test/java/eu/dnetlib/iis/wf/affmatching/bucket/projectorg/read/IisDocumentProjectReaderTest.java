package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import eu.dnetlib.iis.importer.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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
 * @author madryk
 */
@ExtendWith(MockitoExtension.class)
public class IisDocumentProjectReaderTest {
    
    @InjectMocks
    private IisDocumentProjectReader documentProjectReader = new IisDocumentProjectReader();
    
    @Mock
    private SparkAvroLoader avroLoader;
    
    @Mock
    private DocumentProjectConverter documentProjectConverter;
    
    
    @Mock
    private JavaSparkContext sparkContext;
    
    private String inputDocProjPath = "/path/to/document_pojects/";
    
    @Mock
    private JavaRDD<DocumentToProject> documentToProjectRdd;
    
    @Mock
    private JavaRDD<AffMatchDocumentProject> documentProjects;
    
    @Captor
    private ArgumentCaptor<Function<DocumentToProject, AffMatchDocumentProject>> convertDocumentProjectFunction;
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void readDocumentProjects_NULL_CONTEXT() {
        
        // execute
        assertThrows(NullPointerException.class, () -> documentProjectReader.readDocumentProjects(null, inputDocProjPath));
    }
    
    
    @Test
    public void readDocumentProjects_BLANK_PATH() {
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> documentProjectReader.readDocumentProjects(sparkContext, "  "));
    }
    
    
    @Test
    public void readDocumentProjects() throws Exception {
        
        // given
        
        when(avroLoader.loadJavaRDD(sparkContext, inputDocProjPath, DocumentToProject.class)).thenReturn(documentToProjectRdd);
        doReturn(documentProjects).when(documentToProjectRdd).map(any());
        
        
        // execute
        
        JavaRDD<AffMatchDocumentProject> retDocumentProjects = documentProjectReader.readDocumentProjects(sparkContext, inputDocProjPath);
        
        
        // assert

        assertSame(retDocumentProjects, documentProjects);
        
        verify(avroLoader).loadJavaRDD(sparkContext, inputDocProjPath, DocumentToProject.class);
        
        verify(documentToProjectRdd).map(convertDocumentProjectFunction.capture());
        assertConvertDocumentProjectFunction(convertDocumentProjectFunction.getValue());
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void assertConvertDocumentProjectFunction(Function<DocumentToProject, AffMatchDocumentProject> function) throws Exception {
        
        // given
        
        DocumentToProject inputDocProj = mock(DocumentToProject.class);
        AffMatchDocumentProject outputDocProj = mock(AffMatchDocumentProject.class);
        
        when(documentProjectConverter.convert(inputDocProj)).thenReturn(outputDocProj);
        
        // execute
        
        AffMatchDocumentProject retDocProj = function.call(inputDocProj);
        
        // assert

        assertSame(retDocProj, outputDocProj);
    }
}
