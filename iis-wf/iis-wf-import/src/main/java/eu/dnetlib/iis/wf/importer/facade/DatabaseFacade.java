package eu.dnetlib.iis.wf.importer.facade;

/**
 * Database service facade.
 * 
 * @author mhorst
 *
 */
public interface DatabaseFacade {

    /**
     * Delivers all records from given database matching SQL query criteria.
     * 
     * @param databaseName database name the query should be executed on
     * @param query SQL query
     */
    Iterable<String> searchSQL(String databaseName, String query) throws ServiceFacadeException;
    
}
