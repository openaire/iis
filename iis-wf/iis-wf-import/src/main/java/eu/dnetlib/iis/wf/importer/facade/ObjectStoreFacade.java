package eu.dnetlib.iis.wf.importer.facade;

/**
 * ObjectStore service facade.
 * 
 * @author mhorst
 *
 */
public interface ObjectStoreFacade {

    /**
     * Returns metadata records from given objectstore created in specified time range. 
     * @param objectStoreId object store identifier
     * @param from from time in millis
     * @param until until time in millis
     */
    Iterable<String> deliverObjects(String objectStoreId, long from, long until) throws ServiceFacadeException;
    
}
