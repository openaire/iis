package eu.dnetlib.iis.wf.referenceextraction.patent;

import java.util.Map;

import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;

/**
 * Simple factory producing {@link ClassPathBasedPatentServiceFacade}.
 * 
 * @author mhorst
 *
 */
public class ClassPathBasedPatentServiceFacadeFactory implements ServiceFacadeFactory<PatentServiceFacade> {

    @Override
    public PatentServiceFacade instantiate(Map<String, String> parameters) {
        return new ClassPathBasedPatentServiceFacade();
    }
    
    
}
