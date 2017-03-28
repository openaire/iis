package eu.dnetlib.iis.wf.export.actionmanager.entity.facade;

import eu.dnetlib.data.mdstore.DocumentNotFoundException;
import eu.dnetlib.data.mdstore.MDStoreServiceException;

/**
 * MDStore service facade responsible for metadata records delivery.
 * 
 * Each implementation could provide different way of accessing those records e.g. via remote web service, local persistence layer etc.
 * 
 * @author mhorst
 *
 */
public interface MDStoreFacade {

    /**
     * Returns XML metadata record for given MDStore and record identifiers.
     * @param mdStoreId MDStore identifier
     * @param recordId record identifier
     * @throws MDStoreServiceException when general error occurs 
     * @throws DocumentNotFoundException when record not found 
     */
    String fetchRecord(String mdStoreId, String recordId) throws MDStoreServiceException;
    
}
