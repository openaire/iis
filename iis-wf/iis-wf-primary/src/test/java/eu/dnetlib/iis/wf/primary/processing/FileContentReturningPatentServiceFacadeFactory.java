package eu.dnetlib.iis.wf.primary.processing;

import com.google.common.base.Preconditions;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetriever;
import eu.dnetlib.iis.wf.referenceextraction.FacadeContentRetrieverResponse;
import eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory;
import eu.dnetlib.iis.wf.referenceextraction.patent.PatentWebServiceFacadeException;

import java.io.Serializable;
import java.util.Map;

import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.*;

/**
 * Simple factory producing {@link FacadeContentRetriever} for patent metadata.
 * <p>
 * Validates all the parameters required by {@link OpenPatentWebServiceFacadeFactory}.
 */
public class FileContentReturningPatentServiceFacadeFactory implements ServiceFacadeFactory<FacadeContentRetriever<ImportedPatent, String>>, Serializable {

    /**
     * Simple mock retrieving XML contents as files from classpath. Relies on
     * publn_auth, publn_nr, publn_kind fields defined in {@link ImportedPatent}
     * while generating filename:
     * <p>
     * publn_auth + '.' + publn_nr + '.' + publn_kind + ".xml"
     *
     * @author mhorst
     */
    @Override
    public FacadeContentRetriever<ImportedPatent, String> instantiate(Map<String, String> parameters) {
        validateMandatoryParameters(parameters);
        return new FacadeContentRetriever<ImportedPatent, String>() {
            private static final String classPathRoot = "/eu/dnetlib/iis/wf/primary/processing/data/patent/mock_facade_storage/";

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
                try {
                    return FacadeContentRetrieverResponse.success(ClassPathResourceProvider.getResourceContent(
                            classPathRoot + url));
                } catch (Exception e) {
                    throw new PatentWebServiceFacadeException(e.getMessage());
                }
            }
        };
    }

    private static void validateMandatoryParameters(Map<String, String> parameters) {
        checkParameter(parameters, PARAM_CONSUMER_KEY, "expectedConsumerKey");
        checkParameter(parameters, PARAM_CONSUMER_SECRET, "expectedConsumerSecret");

        checkParameter(parameters, PARAM_SERVICE_ENDPOINT_AUTH_HOST, "expectedAuthHost");
        checkParameter(parameters, PARAM_SERVICE_ENDPOINT_AUTH_PORT, "expectedAuthPort");
        checkParameter(parameters, PARAM_SERVICE_ENDPOINT_AUTH_SCHEME, "expectedAuthScheme");
        checkParameter(parameters, PARAM_SERVICE_ENDPOINT_AUTH_URI_ROOT, "expectedAuthUriRoot");

        checkParameter(parameters, PARAM_SERVICE_ENDPOINT_OPS_HOST, "expectedOpsHost");
        checkParameter(parameters, PARAM_SERVICE_ENDPOINT_OPS_PORT, "expectedOpsPort");
        checkParameter(parameters, PARAM_SERVICE_ENDPOINT_OPS_SCHEME, "expectedOpsScheme");
        checkParameter(parameters, PARAM_SERVICE_ENDPOINT_OPS_URI_ROOT, "expectedOpsUriRoot");
    }

    private static void checkParameter(Map<String, String> parameters, String paramName, String expectedValue) {
        String paramValue = parameters.get(paramName);
        Preconditions.checkArgument(expectedValue.equals(paramValue),
                "'%s' parameter value: '%s' is different than the expected one: '%s'", paramName, paramValue, expectedValue);
    }

}
