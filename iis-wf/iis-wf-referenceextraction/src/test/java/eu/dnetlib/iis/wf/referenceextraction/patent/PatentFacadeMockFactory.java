package eu.dnetlib.iis.wf.referenceextraction.patent;

import java.util.Map;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;

/**
 * Simple mock factory producing {@link PatentServiceFacade}.
 * 
 * @author mhorst
 *
 */
public class PatentFacadeMockFactory implements ServiceFacadeFactory<PatentServiceFacade> {
    
    private static final String expectedParamName = "testParam";
    
    private static final String expectedParamValue = "testValue";

    @Override
    public PatentServiceFacade instantiate(Map<String, String> parameters) {
        String paramValue = parameters.get(expectedParamName);
        Preconditions.checkArgument(expectedParamValue.equals(paramValue),
                "'%s' parameter value: '%s' is different than the expected one: '%s'", expectedParamName, paramValue, expectedParamValue);
        return new PatentFacadeMock();
    }
    
    
}
