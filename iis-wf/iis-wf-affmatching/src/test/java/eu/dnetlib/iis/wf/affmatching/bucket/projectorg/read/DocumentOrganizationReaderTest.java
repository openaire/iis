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

import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentOrganization;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchProjectOrganization;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class DocumentOrganizationReaderTest {

    @InjectMocks
    private DocumentOrganizationReader documentOrganizationReader = new DocumentOrganizationReader();
    
    @Mock
    private DocumentProjectReader documentProjectReader;
    
    @Mock
    private ProjectOrganizationReader projectOrganizationReader;
    
    @Mock
    private DocumentOrganizationCombiner documentOrganizationCombiner;
    
    private Float docProjConfidenceLevelThreshold = 0.8f;
    
    
    @Mock
    private JavaSparkContext sc;
    
    private String docProjPath = "/input/doc_proj";
    
    private String projOrgPath = "/input/proj_org";
    
    @Mock
    private JavaRDD<AffMatchDocumentProject> documentProject;
    
    @Mock
    private JavaRDD<AffMatchProjectOrganization> projectOrganization;
    
    @Mock
    private JavaRDD<AffMatchDocumentOrganization> documentOrganizations;
    
    
    @Before
    public void setup() {
        documentOrganizationReader.setDocProjConfidenceLevelThreshold(docProjConfidenceLevelThreshold);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void readDocumentOrganization() {
        
        // given
        
        when(documentProjectReader.readDocumentProject(sc, docProjPath)).thenReturn(documentProject);
        when(projectOrganizationReader.readProjectOrganization(sc, projOrgPath)).thenReturn(projectOrganization);
        when(documentOrganizationCombiner.combine(documentProject, projectOrganization, docProjConfidenceLevelThreshold)).thenReturn(documentOrganizations);
        
        
        // execute
        
        JavaRDD<AffMatchDocumentOrganization> retDocumentOrganizations = documentOrganizationReader.readDocumentOrganization(sc, docProjPath, projOrgPath);
        
        
        // assert
        
        assertTrue(retDocumentOrganizations == documentOrganizations);
        
        verify(documentProjectReader).readDocumentProject(sc, docProjPath);
        verify(projectOrganizationReader).readProjectOrganization(sc, projOrgPath);
        verify(documentOrganizationCombiner).combine(documentProject, projectOrganization, docProjConfidenceLevelThreshold);
        
    }
    
}
