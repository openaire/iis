package eu.dnetlib.iis.wf.referenceextraction.patent;

import java.util.Map;

import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;

/**
 * Simple mock factory producing {@link PatentServiceFacade}.
 * 
 * @author mhorst
 *
 */
public class PatentFacadeMockFactory implements ServiceFacadeFactory<PatentServiceFacade> {

    @Override
    public PatentServiceFacade instantiate(Map<String, String> parameters) {
        return new PatentFacadeMock();
    }
    
    
}
