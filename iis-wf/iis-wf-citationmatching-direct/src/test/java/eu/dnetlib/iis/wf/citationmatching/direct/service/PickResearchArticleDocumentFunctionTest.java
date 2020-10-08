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
public class PickResearchArticleDocumentFunctionTest {

    private PickResearchArticleDocumentFunction documentPickFunction = new PickResearchArticleDocumentFunction();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void pickDocument_WITHOUT_RESEARCH_ARTICLE() throws Exception {
        
        // given
        
        DocumentMetadata firstDocument = new DocumentMetadata("id-1", null, null, Lists.newArrayList());
        DocumentMetadata secondDocument = new DocumentMetadata("id-2", null, null, Lists.newArrayList());
        DocumentMetadata thirdDocument = new DocumentMetadata("id-3", null, null, Lists.newArrayList());
        
        Iterable<DocumentMetadata> documents = Lists.newArrayList(firstDocument, secondDocument, thirdDocument);
        
        
        // execute
        
        DocumentMetadata retDocument = documentPickFunction.call(documents);
        
        
        // assert
        
        assertEquals(thirdDocument, retDocument);
        
    }
    
    @Test
    public void pickDocument_WITH_RESEARCH_ARTICLE() throws Exception {
        
        // given
        
        DocumentMetadata firstDocument = new DocumentMetadata("id-1", null, null, Lists.newArrayList());
        DocumentMetadata secondDocument = new DocumentMetadata("id-2", null, "research-article", Lists.newArrayList());
        DocumentMetadata thirdDocument = new DocumentMetadata("id-3", null, null, Lists.newArrayList());
        
        Iterable<DocumentMetadata> documents = Lists.newArrayList(firstDocument, secondDocument, thirdDocument);
        
        
        // execute
        
        DocumentMetadata retDocument = documentPickFunction.call(documents);
        
        
        // assert
        
        assertEquals(secondDocument, retDocument);
        
    }
    
}
