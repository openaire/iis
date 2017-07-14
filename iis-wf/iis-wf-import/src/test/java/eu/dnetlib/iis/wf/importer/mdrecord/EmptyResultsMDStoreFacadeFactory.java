package eu.dnetlib.iis.wf.importer.mdrecord;

import java.util.Map;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.wf.importer.facade.MDStoreFacade;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeException;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;

/**
 * Factory for building MDStore facade mocks providing empty results.
 * @author mhorst
 *
 */
public class EmptyResultsMDStoreFacadeFactory implements ServiceFacadeFactory<MDStoreFacade> {

    
    //------------------------ LOGIC --------------------------
    
    @Override
    public MDStoreFacade instantiate(Map<String, String> parameters) {
        return new MockMDStoreFacade();
    }
    
    //--------------- INNER CLASS -------------------

    /**
     * MDStore mock providing empty results.
     *
     */
    private static class MockMDStoreFacade implements MDStoreFacade {
        

        //------------------------ LOGIC --------------------------

        @Override
        public Iterable<String> deliverMDRecords(String mdStoreId) throws ServiceFacadeException {
            return Lists.newArrayList();
        }

    }
}

