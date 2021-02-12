package eu.dnetlib.iis.wf.referenceextraction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Service facade content retriever module obtaining content from remote service.
 *
 * @param <U> Type of object to build query url from.
 * @param <C> Type of object for successful response.
 */
public abstract class FacadeContentRetriever<U, C> implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(FacadeContentRetriever.class);

    /**
     * Retrieves content from remove service.
     *
     * @param objToBuildUrl Object to build query url from.
     * @return Successful response with content or persistent/transient failure with exception.
     */
    public FacadeContentRetrieverResponse<C> retrieveContent(U objToBuildUrl) {
        String url = buildUrl(objToBuildUrl);
        long startTime = System.currentTimeMillis();
        log.info("starting content retrieval for url: {}", url);
        try {
            return retrieveContentOrThrow(url, 0);
        } catch (Exception e) {
            log.error("content retrieval failed for url: " + url, e);
            return FacadeContentRetrieverResponse.transientFailure(e);
        } finally {
            log.info("finished content retrieval for url: {} in {} ms", url, (System.currentTimeMillis() - startTime));
        }
    }

    protected abstract String buildUrl(U objToBuildUrl);

    protected abstract FacadeContentRetrieverResponse<C> retrieveContentOrThrow(String url, int retryCount) throws Exception;

    protected FacadeContentRetrieverResponse<C> failureWhenOverMaxRetries(String url, int maxRetriesCount) {
        String message = String.format("number of maximum retries exceeded: '%d' for url: %s", maxRetriesCount, url);
        log.error(message);
        return FacadeContentRetrieverResponse.transientFailure(new RetryLimitExceededException(message));
    }
}
