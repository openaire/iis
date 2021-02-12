package eu.dnetlib.iis.wf.importer.facade;

import java.util.Map;

/**
 * Generic service facade factory. All implementations must be instantiable with no-argument constructor.
 * 
 * @author mhorst
 * 
 */
public interface ServiceFacadeFactory<T> {

    /**
     * Creates service of given type configured with parameters.
     * 
     * @param parameters service configuration
     * 
     */
    T instantiate(Map<String, String> parameters);
}
