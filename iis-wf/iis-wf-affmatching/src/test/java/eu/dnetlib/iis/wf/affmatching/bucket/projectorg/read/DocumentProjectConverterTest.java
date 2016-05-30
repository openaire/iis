package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.dnetlib.iis.importer.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;

/**
 * @author madryk
 */
public class DocumentProjectConverterTest {

    private DocumentProjectConverter converter = new DocumentProjectConverter();
    
    private String documentId = "DOC1";
    private String projectId = "PROJ1";
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void convert_NULL_DOC_PROJ() {
        
        // execute
        converter.convert(null);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void convert_NULL_DOC_ID() {
        
        // execute
        converter.convert(new DocumentToProject(null, projectId));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void convert_BLANK_DOC_ID() {
        
        // execute
        converter.convert(new DocumentToProject(" ", projectId));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void convert_NULL_ORG_ID() {
        
        // execute
        converter.convert(new DocumentToProject(documentId, null));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void convert_BLANK_ORG_ID() {
        
        // execute
        converter.convert(new DocumentToProject(documentId, "  "));
    }
    
    @Test
    public void convert() {
        // given
        DocumentToProject documentProject = new DocumentToProject(documentId, projectId);
        
        // execute
        AffMatchDocumentProject retAffMatchDocProj = converter.convert(documentProject);
        
        // assert
        assertEquals(new AffMatchDocumentProject(documentId, projectId, 1f), retAffMatchDocProj);
    }
    
    
}
