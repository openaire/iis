package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetriever;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetrieverResponse;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

/**
 * Http service facade factories used for testing.
 */
public class TestServiceFacadeFactories {

    private TestServiceFacadeFactories() {
    }

    /**
     * Factory producing content retriever returning content of classpath files. Relies on mappings between url and classpath location.
     */
    public static class FileContentReturningFacadeFactory implements ServiceFacadeFactory<FacadeContentRetriever<String, String>>, Serializable {

        private static final String mappingsLocation = "/eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/content-mappings.properties";

        @Override
        public FacadeContentRetriever<String, String> instantiate(Map<String, String> parameters) {
            Properties urlToClasspathMap = new Properties();
            try {
                urlToClasspathMap.load(ClassPathResourceProvider.getResourceInputStream(mappingsLocation));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return new FacadeContentRetriever<String, String>() {

                @Override
                protected String buildUrl(String objToBuildUrl) {
                    return objToBuildUrl;
                }

                @Override
                protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) {
                    return FacadeContentRetrieverResponse.success(ClassPathResourceProvider.getResourceContent(
                            urlToClasspathMap.getProperty(url)));
                }
            };
        }
    }

    /**
     * Factory producing content retriever returning persistent failure response.
     */
    public static class PersistentFailureReturningFacadeFactory implements ServiceFacadeFactory<FacadeContentRetriever<String, String>>, Serializable {

        @Override
        public FacadeContentRetriever<String, String> instantiate(Map<String, String> parameters) {
            return new FacadeContentRetriever<String, String>() {

                @Override
                protected String buildUrl(String objToBuildUrl) {
                    return objToBuildUrl;
                }

                @Override
                protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) {
                    return FacadeContentRetrieverResponse.persistentFailure(new DocumentNotFoundException());
                }
            };
        }
    }

    /**
     * Factory producing content retriever returning transient failure response.
     */
    public static class TransientFailureReturningFacadeFactory implements ServiceFacadeFactory<FacadeContentRetriever<String, String>>, Serializable {

        @Override
        public FacadeContentRetriever<String, String> instantiate(Map<String, String> parameters) {
            return new FacadeContentRetriever<String, String>() {

                @Override
                protected String buildUrl(String objToBuildUrl) {
                    return objToBuildUrl;
                }

                @Override
                protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) {
                    return FacadeContentRetrieverResponse.transientFailure(new DocumentNotFoundException());
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
