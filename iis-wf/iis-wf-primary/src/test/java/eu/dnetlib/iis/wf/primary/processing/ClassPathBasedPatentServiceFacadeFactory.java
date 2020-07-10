package eu.dnetlib.iis.wf.primary.processing;

import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_CONSUMER_KEY;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_CONSUMER_SECRET;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_AUTH_HOST;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_AUTH_PORT;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_AUTH_SCHEME;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_AUTH_URI_ROOT;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_OPS_HOST;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_OPS_PORT;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_OPS_SCHEME;
import static eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory.PARAM_SERVICE_ENDPOINT_OPS_URI_ROOT;

import java.util.Map;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;
import eu.dnetlib.iis.wf.referenceextraction.patent.OpenPatentWebServiceFacadeFactory;
import eu.dnetlib.iis.wf.referenceextraction.patent.PatentServiceFacade;

/**
 * Simple factory producing {@link ClassPathBasedPatentServiceFacade}.
 * 
 * Validates all the parameters required by {@link OpenPatentWebServiceFacadeFactory}.
 * 
 * @author mhorst
 *
 */
public class ClassPathBasedPatentServiceFacadeFactory implements ServiceFacadeFactory<PatentServiceFacade> {

    @Override
    public PatentServiceFacade instantiate(Map<String, String> parameters) {
        validateMandatoryParameters(parameters);
        return new ClassPathBasedPatentServiceFacade();
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
