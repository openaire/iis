package eu.dnetlib.iis.wf.referenceextraction.patent;

import com.google.common.base.Preconditions;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetriever;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetrieverResponse;

import java.io.Serializable;
import java.util.Map;

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
     *
     * @author mhorst
     */
    public static class FileContentReturningFacadeFactory implements ServiceFacadeFactory<FacadeContentRetriever<ImportedPatent, String>>, Serializable {

        private static final String classPathRoot = "/eu/dnetlib/iis/wf/referenceextraction/patent/data/mock_facade_storage/";

        @Override
        public FacadeContentRetriever<ImportedPatent, String> instantiate(Map<String, String> parameters) {
            return new FacadeContentRetriever<ImportedPatent, String>() {
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
                protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) {
                    return FacadeContentRetrieverResponse.success(ClassPathResourceProvider.getResourceContent(
                            classPathRoot + url));
                }
            };
        }
    }

    /**
     * Simple stub factory producing {@link FacadeContentRetriever} and persistent failure on error.
     */
    public static class StubServiceFacadeFactoryWithPersistentFailure implements ServiceFacadeFactory<FacadeContentRetriever<ImportedPatent, String>>, Serializable {

        private static final String expectedParamName = "testParam";

        private static final String expectedParamValue = "testValue";

        @Override
        public FacadeContentRetriever<ImportedPatent, String> instantiate(Map<String, String> parameters) {
            String paramValue = parameters.get(expectedParamName);
            Preconditions.checkArgument(expectedParamValue.equals(paramValue),
                    "'%s' parameter value: '%s' is different than the expected one: '%s'", expectedParamName, paramValue, expectedParamValue);
            return new FacadeContentRetriever<ImportedPatent, String>() {

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
                protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) {
                    if (url.contains("non-existing")) {
                        return FacadeContentRetrieverResponse.persistentFailure(new PatentWebServiceFacadeException("unable to find element"));
                    }
                    return FacadeContentRetrieverResponse.success(url);
                }
            };
        }
    }

    /**
     * Simple stub factory producing {@link FacadeContentRetriever} and transient failure on error.
     */
    public static class StubServiceFacadeFactoryWithTransientFailure implements ServiceFacadeFactory<FacadeContentRetriever<ImportedPatent, String>>, Serializable {

        private static final String expectedParamName = "testParam";

        private static final String expectedParamValue = "testValue";

        @Override
        public FacadeContentRetriever<ImportedPatent, String> instantiate(Map<String, String> parameters) {
            String paramValue = parameters.get(expectedParamName);
            Preconditions.checkArgument(expectedParamValue.equals(paramValue),
                    "'%s' parameter value: '%s' is different than the expected one: '%s'", expectedParamName, paramValue, expectedParamValue);
            return new FacadeContentRetriever<ImportedPatent, String>() {

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
                protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) {
                    if (url.contains("non-existing")) {
                        return FacadeContentRetrieverResponse.transientFailure(new PatentWebServiceFacadeException("unable to find element"));
                    }
                    return FacadeContentRetrieverResponse.success(url);
                }
            };
        }
    }

    /**
     * Factory throwing an exception.
     */
    public static class ExceptionThrowingFacadeFactory implements ServiceFacadeFactory<FacadeContentRetriever<ImportedPatent, String>>, Serializable {

        @Override
        public FacadeContentRetriever<ImportedPatent, String> instantiate(Map<String, String> parameters) {
            return new FacadeContentRetriever<ImportedPatent, String>() {
                @Override
                protected String buildUrl(ImportedPatent objToBuildUrl) {
                    return "/url/to/patent";
                }

                @Override
                protected FacadeContentRetrieverResponse<String> retrieveContentOrThrow(String url, int retryCount) {
                    throw new RuntimeException("unexpected call for url: " + url);
                }
            };
        }
    }
}
