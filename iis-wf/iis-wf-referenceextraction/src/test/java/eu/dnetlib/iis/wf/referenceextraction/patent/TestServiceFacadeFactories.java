package eu.dnetlib.iis.wf.referenceextraction.patent;

import com.google.common.base.Preconditions;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetriever;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetrieverResponse;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Patent service facade factories used for testing.
 */
public class TestServiceFacadeFactories {

    private TestServiceFacadeFactories() {
    }

    /**
     * Simple mock retrieving XML contents as files from classpath. Relies on
     * publn_auth, publn_nr, publn_kind fields defined in {@link ImportedPatent}
     * while generating filename:
     * <p>
     * publn_auth + '.' + publn_nr + '.' + publn_kind + ".xml"
     * <p>
     * Throws an exception if a content is retrieved more than once.
     *
     * @author mhorst
     */
    public static class FileContentReturningFacadeFactory implements ServiceFacadeFactory<FacadeContentRetriever<ImportedPatent, String>>, Serializable {

        private static final String classPathRoot = "/eu/dnetlib/iis/wf/referenceextraction/patent/data/mock_facade_storage/";

        public static class Retriever extends FacadeContentRetriever<ImportedPatent, String> {
            public final Set<String> retrievedUrls = new HashSet<>();

            @Override
            protected String buildUrl(ImportedPatent objToBuildUrl) {
                StringBuilder strBuilder = new StringBuilder();
                strBuilder.append(objToBuildUrl.getPublnAuth());
                strBuilder.append('.');
                strBuilder.append(objToBuildUrl.getPublnNr());
                strBuilder.append('.');
                strBuilder.append(objToBuildUrl.getPublnKind());
                strBuilder.append(".xml");
                return strBuilder.toString();
            }

            @Override
            protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) throws Exception {
                if (retrievedUrls.contains(url)) {
                    throw new RuntimeException("requesting already processed url: " + url);
                }
                FacadeContentRetrieverResponse.Success<String> result = FacadeContentRetrieverResponse
                        .success(ClassPathResourceProvider.getResourceContent(classPathRoot + url));
                retrievedUrls.add(url);
                return result;
            }
        }

        @Override
        public FacadeContentRetriever<ImportedPatent, String> instantiate(Map<String, String> parameters) {
            return new Retriever();
        }
    }

    private static abstract class StubServiceFacadeFactoryWithFailure implements ServiceFacadeFactory<FacadeContentRetriever<ImportedPatent, String>>, Serializable {
        private static final String expectedParamName = "testParam";
        private static final String expectedParamValue = "testValue";

        public static class Retriever extends FacadeContentRetriever<ImportedPatent, String> {
            private final Set<String> retrievedUrls = new HashSet<>();
            private final Function<Exception, FacadeContentRetrieverResponse.Failure<String>> failureProducerFn;

            public Retriever(Function<Exception, FacadeContentRetrieverResponse.Failure<String>> failureProducerFn) {
                this.failureProducerFn = failureProducerFn;
            }

            @Override
            protected String buildUrl(ImportedPatent objToBuildUrl) {
                StringBuilder strBuilder = new StringBuilder();
                strBuilder.append(objToBuildUrl.getApplnAuth());
                strBuilder.append('-');
                strBuilder.append(objToBuildUrl.getApplnNr());
                strBuilder.append('-');
                strBuilder.append(objToBuildUrl.getPublnAuth());
                strBuilder.append('-');
                strBuilder.append(objToBuildUrl.getPublnNr());
                strBuilder.append('-');
                strBuilder.append(objToBuildUrl.getPublnKind());
                return strBuilder.toString();
            }

            @Override
            protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) throws Exception {
                if (retrievedUrls.contains(url)) {
                    throw new RuntimeException("requesting already processed url: " + url);
                }
                FacadeContentRetrieverResponse<String> result = url.contains("non-existing") ?
                        failureProducerFn.apply(new PatentWebServiceFacadeException("unable to find element"))
                        : FacadeContentRetrieverResponse.success(url);
                retrievedUrls.add(url);
                return result;
            }
        }

        protected abstract FacadeContentRetrieverResponse.Failure<String> failure(Exception e);

        @Override
        public FacadeContentRetriever<ImportedPatent, String> instantiate(Map<String, String> parameters) {
            String paramValue = parameters.get(expectedParamName);
            Preconditions.checkArgument(expectedParamValue.equals(paramValue),
                    "'%s' parameter value: '%s' is different than the expected one: '%s'", expectedParamName, paramValue, expectedParamValue);
            return new Retriever((Function<Exception, FacadeContentRetrieverResponse.Failure<String>> & Serializable) this::failure);
        }
    }

    /**
     * Simple stub factory producing {@link FacadeContentRetriever} and persistent failure on error. Throws an exception
     * if a content is retrieved more than once.
     */
    public static class StubServiceFacadeFactoryWithPersistentFailure extends StubServiceFacadeFactoryWithFailure {

        @Override
        protected FacadeContentRetrieverResponse.Failure<String> failure(Exception e) {
            return FacadeContentRetrieverResponse.persistentFailure(e);
        }
    }

    /**
     * Simple stub factory producing {@link FacadeContentRetriever} and transient failure on error. Throws an exception
     * if a content is retrieved more than once.
     */
    public static class StubServiceFacadeFactoryWithTransientFailure extends StubServiceFacadeFactoryWithFailure {

        @Override
        protected FacadeContentRetrieverResponse.Failure<String> failure(Exception e) {
            return FacadeContentRetrieverResponse.transientFailure(e);
        }
    }

    /**
     * Factory throwing an exception.
     */
    public static class ExceptionThrowingFacadeFactory implements ServiceFacadeFactory<FacadeContentRetriever<ImportedPatent, String>>, Serializable {

        public static class Retriever extends FacadeContentRetriever<ImportedPatent, String> {

            @Override
            protected String buildUrl(ImportedPatent objToBuildUrl) {
                return "/url/to/patent";
            }

            @Override
            protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) throws Exception {
                throw new RuntimeException("unexpected call for url: " + url);
            }
        }

        @Override
        public FacadeContentRetriever<ImportedPatent, String> instantiate(Map<String, String> parameters) {
            return new Retriever();
        }
    }
}
