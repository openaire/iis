package eu.dnetlib.iis.workflows.documentsclassification;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.dnetlib.iis.documentsclassification.schemas.DocumentMetadata;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;

/**
 * @author Łukasz Dumiszewski
 */

public class DocumentToDocClassificationMetadataConverterTest {

    private DocumentToDocClassificationMetadataConverter converter = new DocumentToDocClassificationMetadataConverter();
    
    
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test(expected = NullPointerException.class)
    public void convert_Null_Document() {
        
        // execute
        
        converter.convert(null);
        
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
