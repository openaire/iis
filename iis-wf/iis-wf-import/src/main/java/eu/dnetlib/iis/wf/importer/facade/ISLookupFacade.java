package eu.dnetlib.iis.wf.importer.facade;

/**
 * ISLookup service facade.
 * 
 * @author mhorst
 *
 */
public interface ISLookupFacade {

    /**
     * Provides all profiles matching given query
     * @param xPathQuery XPath query
     */
    Iterable<String> searchProfile(String xPathQuery) throws ServiceFacadeException;
    
}
