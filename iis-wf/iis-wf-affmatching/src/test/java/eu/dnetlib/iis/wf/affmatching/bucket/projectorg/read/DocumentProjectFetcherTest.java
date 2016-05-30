package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class DocumentProjectFetcherTest {

    @InjectMocks
    private DocumentProjectFetcher documentProjectFetcher = new DocumentProjectFetcher();
    
    @Mock
    private DocumentProjectReader firstDocumentProjectReader;
    
    @Mock
    private DocumentProjectReader secondDocumentProjectReader;
    
    @Mock
    private DocumentProjectMerger documentProjectMerger;
    
    
    @Mock
    private JavaSparkContext sparkContext;
    
    
    private String firstDocProjPath = "/docproj/first";
    
    private String secondDocProjPath = "/docproj/second";
    
    
    @Mock
    private JavaRDD<AffMatchDocumentProject> firstDocumentProjects;
    
    @Mock
    private JavaRDD<AffMatchDocumentProject> secondDocumentProjects;
    
    @Mock
    private JavaRDD<AffMatchDocumentProject> mergedDocumentProjects;
    
    
    @Before
    public void setup() {
        documentProjectFetcher.setFirstDocProjPath(firstDocProjPath);
        documentProjectFetcher.setSecondDocProjPath(secondDocProjPath);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void fetchDocumentProjects() {
        
        // given
        
        when(firstDocumentProjectReader.readDocumentProjects(sparkContext, firstDocProjPath)).thenReturn(firstDocumentProjects);
        when(secondDocumentProjectReader.readDocumentProjects(sparkContext, secondDocProjPath)).thenReturn(secondDocumentProjects);
        when(documentProjectMerger.merge(firstDocumentProjects, secondDocumentProjects)).thenReturn(mergedDocumentProjects);
        
        // execute
        
        JavaRDD<AffMatchDocumentProject> retDocumentProjects = documentProjectFetcher.fetchDocumentProjects();
        
        // assert
        
        assertTrue(retDocumentProjects == mergedDocumentProjects);
        
        verify(firstDocumentProjectReader).readDocumentProjects(sparkContext, firstDocProjPath);
        verify(secondDocumentProjectReader).readDocumentProjects(sparkContext, secondDocProjPath);
        verify(documentProjectMerger).merge(firstDocumentProjects, secondDocumentProjects);
    }
}
