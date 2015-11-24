package eu.dnetlib.iis.workflows.citationmatching.direct.service;

import org.apache.spark.api.java.function.Function;

import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;

/**
 * Function that picks first document from given documents iterable
 * 
 * @author madryk
 *
 */
public class PickFirstDocumentFunction implements Function<Iterable<DocumentMetadata>, DocumentMetadata> {

    private static final long serialVersionUID = 1L;


    //------------------------ LOGIC --------------------------

    @Override
    public DocumentMetadata call(Iterable<DocumentMetadata> documents) throws Exception {
        
        return documents.iterator().next();
    }


}
