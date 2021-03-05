package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetriever;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetrieverResponse;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Http service facade factories used for testing.
 */
public class TestServiceFacadeFactories {

    private TestServiceFacadeFactories() {
    }

    /**
     * Factory producing content retriever returning content of classpath files. Relies on mappings between url and
     * classpath location. Throws an exception if a content is retrieved more than once.
     */
    public static class FileContentReturningFacadeFactory implements ServiceFacadeFactory<FacadeContentRetriever<String, String>>, Serializable {

        private static final String mappingsLocation = "/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/content-mappings.properties";

        public static Set<String> retrievedUrls;

        @Override
        public FacadeContentRetriever<String, String> instantiate(Map<String, String> parameters) {
            Properties urlToClasspathMap = new Properties();
            try {
                urlToClasspathMap.load(ClassPathResourceProvider.getResourceInputStream(mappingsLocation));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            retrievedUrls = new HashSet<>();

            return new FacadeContentRetriever<String, String>() {

                @Override
                protected String buildUrl(String objToBuildUrl) {
                    return objToBuildUrl;
                }

                @Override
                protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) {
                    if (retrievedUrls.contains(url)) {
                        throw new RuntimeException("requesting already processed url: " + url);
                    }
                    FacadeContentRetrieverResponse.Success<String> result = FacadeContentRetrieverResponse
                            .success(ClassPathResourceProvider.getResourceContent(urlToClasspathMap.getProperty(url)));
                    retrievedUrls.add(url);
                    return result;
                }
            };
        }
    }

    /**
     * Factory producing content retriever returning persistent failure response. Throws an exception if a content is
     * retrieved more than once.
     */
    public static class PersistentFailureReturningFacadeFactory implements ServiceFacadeFactory<FacadeContentRetriever<String, String>>, Serializable {

        public static Set<String> retrievedUrls;

        @Override
        public FacadeContentRetriever<String, String> instantiate(Map<String, String> parameters) {
            retrievedUrls = new HashSet<>();

            return new FacadeContentRetriever<String, String>() {

                @Override
                protected String buildUrl(String objToBuildUrl) {
                    return objToBuildUrl;
                }

                @Override
                protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) {
                    if (retrievedUrls.contains(url)) {
                        throw new RuntimeException("requesting already processed url: " + url);
                    }
                    FacadeContentRetrieverResponse.Failure<String> result = FacadeContentRetrieverResponse
                            .persistentFailure(new DocumentNotFoundException());
                    retrievedUrls.add(url);
                    return result;
                }
            };
        }
    }

    /**
     * Factory producing content retriever returning transient failure response. Throws an exception if a content is
     * retrieved more than once.
     */
    public static class TransientFailureReturningFacadeFactory implements ServiceFacadeFactory<FacadeContentRetriever<String, String>>, Serializable {

        public static Set<String> retrievedUrls;

        @Override
        public FacadeContentRetriever<String, String> instantiate(Map<String, String> parameters) {
            retrievedUrls = new HashSet<>();

            return new FacadeContentRetriever<String, String>() {

                @Override
                protected String buildUrl(String objToBuildUrl) {
                    return objToBuildUrl;
                }

                @Override
                protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) {
                    if (retrievedUrls.contains(url)) {
                        throw new RuntimeException("requesting already processed url: " + url);
                    }
                    FacadeContentRetrieverResponse.Failure<String> result = FacadeContentRetrieverResponse
                            .transientFailure(new DocumentNotFoundException());
                    retrievedUrls.add(url);
                    return result;
                }
            };
        }
    }

    /**
     * Factory throwing an exception.
     */
    public static class ExceptionThrowingFacadeFactory implements ServiceFacadeFactory<FacadeContentRetriever<String, String>>, Serializable {

        @Override
        public FacadeContentRetriever<String, String> instantiate(Map<String, String> parameters) {
            return new FacadeContentRetriever<String, String>() {

                @Override
                protected String buildUrl(String objToBuildUrl) {
                    return objToBuildUrl;
                }

                @Override
                protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) {
                    throw new RuntimeException("unexpected content retrieval call!");
                }
            };
        }
    }
}
