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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.importer.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;

/**
 * @author madryk
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
    
    private String inputDocProjPath = "/path/to/document_pojects/";
    
    @Mock
    private JavaRDD<DocumentToProject> documentToProjectRdd;
    
    @Mock
    private JavaRDD<AffMatchDocumentProject> documentProjects;
    
    @Captor
    private ArgumentCaptor<Function<DocumentToProject, AffMatchDocumentProject>> convertDocumentProjectFunction;
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void readDocumentProjects_NULL_CONTEXT() {
        
        // execute
        documentProjectReader.readDocumentProjects(null, inputDocProjPath);
    }
    
    
    @Test(expected = IllegalArgumentException.class)
    public void readDocumentProjects_BLANK_PATH() {
        
        // execute
        documentProjectReader.readDocumentProjects(sparkContext, "  ");
    }
    
    
    @Test
    public void readDocumentProjects() throws Exception {
        
        // given
        
        when(avroLoader.loadJavaRDD(sparkContext, inputDocProjPath, DocumentToProject.class)).thenReturn(documentToProjectRdd);
        doReturn(documentProjects).when(documentToProjectRdd).map(any());
        
        
        // execute
        
        JavaRDD<AffMatchDocumentProject> retDocumentProjects = documentProjectReader.readDocumentProjects(sparkContext, inputDocProjPath);
        
        
        // assert
        
        assertTrue(retDocumentProjects == documentProjects);
        
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
        
        assertTrue(retDocProj == outputDocProj);
    }
}
