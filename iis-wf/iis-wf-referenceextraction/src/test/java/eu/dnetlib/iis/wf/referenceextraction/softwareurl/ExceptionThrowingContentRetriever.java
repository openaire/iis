package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import eu.dnetlib.iis.wf.referenceextraction.ContentRetrieverResponse;

/**
 * Content retriever throwing {@link RuntimeException} for each call. 
 * To be used for testing caching functionality where content retrieval should not be triggered.
 * 
 * @author mhorst
 *
 */
public class ExceptionThrowingContentRetriever implements ContentRetriever {

    /**
     * 
     */
    private static final long serialVersionUID = -5244888543422890414L;

    @Override
    public ContentRetrieverResponse retrieveUrlContent(CharSequence url, int connectionTimeout, int readTimeout,
            int maxPageContentLength) {
        throw new RuntimeException("unexpected content retrieval call!");
    }

}
