package eu.dnetlib.iis.wf.importer.facade;

/**
 * ObjectStore service facade.
 * 
 * @author mhorst
 *
 */
public interface ObjectStoreFacade {

    /**
     * Provides all metadata records from given objectstore. 
     * @param objectStoreId object store identifier
     * @param from from time in millis
     * @param until until time in millis
     */
    Iterable<String> deliverObjects(String objectStoreId, long from, long until) throws ServiceFacadeException;
    
}
