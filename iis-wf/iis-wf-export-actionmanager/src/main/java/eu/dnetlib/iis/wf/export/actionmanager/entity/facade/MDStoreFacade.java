package eu.dnetlib.iis.wf.export.actionmanager.entity.facade;

import eu.dnetlib.data.mdstore.DocumentNotFoundException;
import eu.dnetlib.data.mdstore.MDStoreServiceException;

/**
 * MDStore service facade.
 * 
 * @author mhorst
 *
 */
public interface MDStoreFacade {

    /**
     * Delivers single record for given MDStore and record identifiers.
     * @param mdStoreId MDStore identifier
     * @param recordId record identifier
     * @throws MDStoreServiceException when general error occurs 
     * @throws DocumentNotFoundException when record not found 
     */
    public String deliverRecord(String mdStoreId, String recordId) throws MDStoreServiceException;
    
}
