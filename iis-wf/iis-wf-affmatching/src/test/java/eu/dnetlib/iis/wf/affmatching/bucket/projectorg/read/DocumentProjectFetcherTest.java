package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author madryk
 */
@ExtendWith(MockitoExtension.class)
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
    
    
    @BeforeEach
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

        assertSame(retDocumentProjects, mergedDocumentProjects);
        
        verify(firstDocumentProjectReader).readDocumentProjects(sparkContext, firstDocProjPath);
        verify(secondDocumentProjectReader).readDocumentProjects(sparkContext, secondDocProjPath);
        verify(documentProjectMerger).merge(firstDocumentProjects, secondDocumentProjects);
    }
}
