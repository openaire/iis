package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import eu.dnetlib.iis.importer.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author madryk
 */
public class DocumentProjectConverterTest {

    private DocumentProjectConverter converter = new DocumentProjectConverter();
    
    private String documentId = "DOC1";
    private String projectId = "PROJ1";
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void convert_NULL_DOC_PROJ() {
        
        // execute
        assertThrows(NullPointerException.class, () -> converter.convert(null));
    }
    
    @Test
    public void convert_NULL_DOC_ID() {
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> converter.convert(new DocumentToProject(null, projectId)));
    }
    
    @Test
    public void convert_BLANK_DOC_ID() {
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> converter.convert(new DocumentToProject(" ", projectId)));
    }
    
    @Test
    public void convert_NULL_ORG_ID() {
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> converter.convert(new DocumentToProject(documentId, null)));
    }
    
    @Test
    public void convert_BLANK_ORG_ID() {
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> converter.convert(new DocumentToProject(documentId, "  ")));
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
