package eu.dnetlib.iis.wf.importer.concept;

import java.util.Map;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.wf.importer.facade.ISLookupFacade;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeException;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;

/**
 * Factory for building ISLookup facade mocks.
 * @author mhorst
 *
 */
public class EmptyResultsISLookupFacadeFactory implements ServiceFacadeFactory<ISLookupFacade> {

    
    //------------------------ LOGIC --------------------------
    
    @Override
    public ISLookupFacade instantiate(Map<String, String> parameters) {
        return new MockISLookupFacade();
    }
    
    //--------------- INNER CLASS -------------------

    /**
     * ISLookup mock providing static concept profiles.
     *
     */
    private static class MockISLookupFacade implements ISLookupFacade {
        
        //------------------------ LOGIC --------------------------
        
        @Override
        public Iterable<String> searchProfile(String xPathQuery) throws ServiceFacadeException {
            return Lists.newArrayList("");
        }

    }
}
