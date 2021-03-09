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

        public static class Retriever extends FacadeContentRetriever<String, String> {
            private final Properties urlToClasspathMap = new Properties();
            private final Set<String> retrievedUrls = new HashSet<>();

            public Retriever() {
                try {
                    urlToClasspathMap.load(ClassPathResourceProvider.getResourceInputStream(mappingsLocation));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            protected String buildUrl(String objToBuildUrl) {
                return objToBuildUrl;
            }

            @Override
            protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) throws Exception {
                if (retrievedUrls.contains(url)) {
                    throw new RuntimeException("requesting already processed url: " + url);
                }
                FacadeContentRetrieverResponse.Success<String> result = FacadeContentRetrieverResponse
                        .success(ClassPathResourceProvider.getResourceContent(urlToClasspathMap.getProperty(url)));
                retrievedUrls.add(url);
                return result;
            }
        }

        @Override
        public FacadeContentRetriever<String, String> instantiate(Map<String, String> parameters) {
            return new Retriever();
        }
    }

    /**
     * Factory producing content retriever returning persistent failure response. Throws an exception if a content is
     * retrieved more than once.
     */
    public static class PersistentFailureReturningFacadeFactory implements ServiceFacadeFactory<FacadeContentRetriever<String, String>>, Serializable {

        public static class Retriever extends FacadeContentRetriever<String, String> {
            public final Set<String> retrievedUrls = new HashSet<>();

            @Override
            protected String buildUrl(String objToBuildUrl) {
                return objToBuildUrl;
            }

            @Override
            protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) throws Exception {
                if (retrievedUrls.contains(url)) {
                    throw new RuntimeException("requesting already processed url: " + url);
                }
                FacadeContentRetrieverResponse.Failure<String> result = FacadeContentRetrieverResponse
                        .persistentFailure(new DocumentNotFoundException());
                retrievedUrls.add(url);
                return result;
            }
        }

        @Override
        public FacadeContentRetriever<String, String> instantiate(Map<String, String> parameters) {
            return new Retriever();
        }
    }

    /**
     * Factory producing content retriever returning transient failure response. Throws an exception if a content is
     * retrieved more than once.
     */
    public static class TransientFailureReturningFacadeFactory implements ServiceFacadeFactory<FacadeContentRetriever<String, String>>, Serializable {

        public static class Retriever extends FacadeContentRetriever<String, String> {
            public final Set<String> retrievedUrls = new HashSet<>();

            @Override
            protected String buildUrl(String objToBuildUrl) {
                return objToBuildUrl;
            }

            @Override
            protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) throws Exception {
                if (retrievedUrls.contains(url)) {
                    throw new RuntimeException("requesting already processed url: " + url);
                }
                FacadeContentRetrieverResponse.Failure<String> result = FacadeContentRetrieverResponse
                        .transientFailure(new DocumentNotFoundException());
                retrievedUrls.add(url);
                return result;
            }
        }

        @Override
        public FacadeContentRetriever<String, String> instantiate(Map<String, String> parameters) {
            return new Retriever();
        }
    }

    /**
     * Factory throwing an exception.
     */
    public static class ExceptionThrowingFacadeFactory implements ServiceFacadeFactory<FacadeContentRetriever<String, String>>, Serializable {

        public static class Retriever extends FacadeContentRetriever<String, String> {

            @Override
            protected String buildUrl(String objToBuildUrl) {
                return objToBuildUrl;
            }

            @Override
            protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) throws Exception {
                throw new RuntimeException("unexpected content retrieval call!");
            }
        }

        @Override
        public FacadeContentRetriever<String, String> instantiate(Map<String, String> parameters) {
            return new Retriever();
        }
    }
}
