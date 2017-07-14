package eu.dnetlib.iis.wf.importer.mdrecord;

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
        
        //------------------------ LOGIC --------------------------

        @Override
        public Iterable<String> deliverMDRecords(String mdStoreId) throws ServiceFacadeException {
            return StaticResourcesProvider.getResources(
                    "/eu/dnetlib/iis/wf/importer/mdrecord/data/input/mdrecord_1.xml",
                    "/eu/dnetlib/iis/wf/importer/mdrecord/data/input/mdrecord_2.xml",
                    "/eu/dnetlib/iis/wf/importer/mdrecord/data/input/mdrecord_3_no_id.xml");
        }

    }
}

