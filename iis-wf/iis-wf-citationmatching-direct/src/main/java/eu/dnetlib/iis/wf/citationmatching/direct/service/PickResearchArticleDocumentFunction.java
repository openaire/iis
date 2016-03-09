package eu.dnetlib.iis.wf.citationmatching.direct.service;

import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.Function;

import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;

/**
 * Function that picks first research-article document from documents iterable.
 * If iterable does not contain research-article document than the last document
 * from iterable will be picked 
 * 
 * @author madryk
 *
 */
public class PickResearchArticleDocumentFunction implements Function<Iterable<DocumentMetadata>, DocumentMetadata> {

    private static final long serialVersionUID = 1L;



    //------------------------ LOGIC --------------------------
    
    @Override
    public DocumentMetadata call(Iterable<DocumentMetadata> documents) throws Exception {
        
        Iterator<DocumentMetadata> it = documents.iterator();
        
        DocumentMetadata current = null;
        while(it.hasNext()) {
            DocumentMetadata docMeta = it.next();
            if (StringUtils.equals(docMeta.getPublicationTypeName(), "research-article")) {
                return docMeta;
            }
            
            current = docMeta;
        }
        return current;
        
    }

    
}
