package eu.dnetlib.iis.wf.importer.content;

import java.util.Map;

import eu.dnetlib.iis.wf.importer.facade.ObjectStoreFacade;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeException;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;

/**
 * Factory for building ObjectStore facade mock throwing exception.
 * 
 * @author mhorst
 *
 */
public class ExceptionThrowingObjectStoreFacadeFactory implements ServiceFacadeFactory<ObjectStoreFacade> {

    
    //------------------------ LOGIC --------------------------
    
    @Override
    public ObjectStoreFacade instantiate(Map<String, String> parameters) {
        return new MockObjectStoreFacade();
    }
    
    //--------------- INNER CLASS -------------------

    /**
     * ObjectStore mock providing static metadata records.
     *
     */
    private static class MockObjectStoreFacade implements ObjectStoreFacade {

        //------------------------ LOGIC --------------------------

        @Override
        public Iterable<String> deliverObjects(String objectStoreId, long from, long until)
                throws ServiceFacadeException {
            throw new ServiceFacadeException("invalid object store id: " + objectStoreId);
        }

    }
}

