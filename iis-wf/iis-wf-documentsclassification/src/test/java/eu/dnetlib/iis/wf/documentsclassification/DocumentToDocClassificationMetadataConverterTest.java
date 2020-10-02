package eu.dnetlib.iis.wf.documentsclassification;

import eu.dnetlib.iis.documentsclassification.schemas.DocumentMetadata;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Łukasz Dumiszewski
 */

public class DocumentToDocClassificationMetadataConverterTest {

    private DocumentToDocClassificationMetadataConverter converter = new DocumentToDocClassificationMetadataConverter();
    
    
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test
    public void convert_Null_Document() {
        
        // execute
        assertThrows(NullPointerException.class, () -> converter.convert(null));
        
    }
    
    
    
    @Test
    public void convert() {
        
        
        // given
        
        ExtractedDocumentMetadataMergedWithOriginal document = new ExtractedDocumentMetadataMergedWithOriginal();
        document.setId("XYZ");
        document.setAbstract$("This is an abstract");
        
        
        // execute
        
        DocumentMetadata metadata = converter.convert(document);

        
        // assert
        
        assertEquals(document.getId(), metadata.getId());
        assertEquals(document.getAbstract$(), metadata.getAbstract$());
        
    }

    
}
