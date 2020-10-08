package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentOrganization;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchProjectOrganization;
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
public class DocumentOrganizationFetcherTest {

    @InjectMocks
    private DocumentOrganizationFetcher documentOrganizationFetcher = new DocumentOrganizationFetcher();
    
    @Mock
    private ProjectOrganizationReader projectOrganizationReader;
    
    @Mock
    private DocumentProjectFetcher documentProjectFetcher;
    
    @Mock
    private DocumentOrganizationCombiner documentOrganizationCombiner;
    
    private Float docProjConfidenceLevelThreshold = 0.8f;
    
    
    @Mock
    private JavaSparkContext sc;
    
    
    private String projOrgPath = "/input/proj_org";
    
    @Mock
    private JavaRDD<AffMatchDocumentProject> documentProject;
    
    @Mock
    private JavaRDD<AffMatchProjectOrganization> projectOrganization;
    
    @Mock
    private JavaRDD<AffMatchDocumentOrganization> documentOrganizations;
    
    
    @BeforeEach
    public void setup() {
        documentOrganizationFetcher.setDocProjConfidenceLevelThreshold(docProjConfidenceLevelThreshold);
        documentOrganizationFetcher.setProjOrgPath(projOrgPath);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void fetchDocumentOrganizations() {
        
        // given
        
        when(documentProjectFetcher.fetchDocumentProjects()).thenReturn(documentProject);
        when(projectOrganizationReader.readProjectOrganizations(sc, projOrgPath)).thenReturn(projectOrganization);
        when(documentOrganizationCombiner.combine(documentProject, projectOrganization, docProjConfidenceLevelThreshold)).thenReturn(documentOrganizations);
        
        
        // execute
        
        JavaRDD<AffMatchDocumentOrganization> retDocumentOrganizations = documentOrganizationFetcher.fetchDocumentOrganizations();
        
        
        // assert

        assertSame(retDocumentOrganizations, documentOrganizations);
        
        verify(documentProjectFetcher).fetchDocumentProjects();
        verify(projectOrganizationReader).readProjectOrganizations(sc, projOrgPath);
        verify(documentOrganizationCombiner).combine(documentProject, projectOrganization, docProjConfidenceLevelThreshold);
        
    }
    
}
