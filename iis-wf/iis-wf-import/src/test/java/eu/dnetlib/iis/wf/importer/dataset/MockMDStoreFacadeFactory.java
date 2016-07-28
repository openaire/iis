package eu.dnetlib.iis.wf.importer.dataset;

import java.util.Map;

import eu.dnetlib.iis.wf.importer.StaticResourcesProvider;
import eu.dnetlib.iis.wf.importer.facade.MDStoreFacade;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeException;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;

/**
 * Factory for building MDStore facade mocks.
 * @author mhorst
 *
 */
public class MockMDStoreFacadeFactory implements ServiceFacadeFactory<MDStoreFacade> {

    
    //------------------------ LOGIC --------------------------
    
    @Override
    public MDStoreFacade instantiate(Map<String, String> parameters) {
        return new MockMDStoreFacade();
    }
    
    //--------------- INNER CLASS -------------------

    /**
     * MDStore mock providing static DMF records.
     *
     */
    private static class MockMDStoreFacade implements MDStoreFacade {
        
        private static final String profileLocation = "/eu/dnetlib/iis/wf/importer/dataset/data/input/datacite_test_dump.xml";

        //------------------------ LOGIC --------------------------

        @Override
        public Iterable<String> deliverMDRecords(String mdStoreId) throws ServiceFacadeException {
            return StaticResourcesProvider.getResources(profileLocation);
        }

    }
}

