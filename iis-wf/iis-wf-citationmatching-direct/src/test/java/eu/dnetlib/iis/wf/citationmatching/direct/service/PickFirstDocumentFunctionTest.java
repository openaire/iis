package eu.dnetlib.iis.wf.citationmatching.direct.service;

import com.google.common.collect.Lists;
import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 
 * @author madryk
 *
 */
public class PickFirstDocumentFunctionTest {

    private PickFirstDocumentFunction documentPickFunction = new PickFirstDocumentFunction();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void pickDocument() throws Exception {
        
        // given
        
        DocumentMetadata firstDocument = new DocumentMetadata("id-1", null, null, Lists.newArrayList());
        DocumentMetadata secondDocument = new DocumentMetadata("id-2", null, null, Lists.newArrayList());
        DocumentMetadata thirdDocument = new DocumentMetadata("id-3", null, null, Lists.newArrayList());
        
        Iterable<DocumentMetadata> documents = Lists.newArrayList(firstDocument, secondDocument, thirdDocument);
        
        
        // execute
        
        DocumentMetadata retDocument = documentPickFunction.call(documents);
        
        
        // assert
        
        assertEquals(firstDocument, retDocument);
        
    }
}
