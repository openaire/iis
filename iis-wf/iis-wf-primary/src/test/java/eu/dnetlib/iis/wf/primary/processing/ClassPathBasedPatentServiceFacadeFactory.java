package eu.dnetlib.iis.wf.primary.processing;

import java.util.Map;

import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;
import eu.dnetlib.iis.wf.referenceextraction.patent.PatentServiceFacade;

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
