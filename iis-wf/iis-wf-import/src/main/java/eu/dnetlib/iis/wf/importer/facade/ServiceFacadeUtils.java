package eu.dnetlib.iis.wf.importer.facade;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_FACADE_FACTORY_CLASS;

import java.lang.reflect.Constructor;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableMap;

import eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters;

/**
 * Service facade utility methods.
 * @author mhorst
 *
 */
public abstract class ServiceFacadeUtils {

    //------------------------ LOGIC --------------------------
    
    /**
     * Instantiates service based on provided parameters.
     * 
     * Service factory class name is mandatory and has to be provided as {@value ImportWorkflowRuntimeParameters#IMPORT_FACADE_FACTORY_CLASS} parameter.
     * Other parameters will be used by factory itself. Factory must be instantiable with no-argument construtor.
     * 
     * @param parameters set of parameters required for service instantiation
     * 
     */
    public static <T> T instantiate(Map<String, String> parameters) throws ServiceFacadeException {
        String serviceFactoryClassName = parameters.get(IMPORT_FACADE_FACTORY_CLASS);
        if (StringUtils.isBlank(serviceFactoryClassName)) {
            throw new ServiceFacadeException("unknown service facade factory, no " + IMPORT_FACADE_FACTORY_CLASS + " parameter provided!");
        }
        try {
            Class<?> clazz = Class.forName(serviceFactoryClassName);
            Constructor<?> constructor = clazz.getConstructor();
            @SuppressWarnings("unchecked")
            ServiceFacadeFactory<T> serviceFactory = (ServiceFacadeFactory<T>) constructor.newInstance();
            return serviceFactory.instantiate(parameters);    
        } catch (Exception e) {
            throw new ServiceFacadeException("exception occurred while instantiating service by facade factory: " + IMPORT_FACADE_FACTORY_CLASS, e);
        }
        
    }
    
    /**
     * Instantiates service based on provided configuration.
     * 
     * Service factory class name is mandatory and has to be provided as {@value ImportWorkflowRuntimeParameters#IMPORT_FACADE_FACTORY_CLASS} configuration entry.
     * Other parameters will be used by factory itself. Factory must be instantiable with no-argument construtor.
     * 
     * @param config set of configuration entries required for service instantiation
     */
    public static <T> T instantiate(Configuration config) throws ServiceFacadeException {
        return instantiate(buildParameters(config));
    }
    

    // ------------------------ PRIVATE --------------------------
    
    /**
     * Converts configuration entries into plain map.
     */
    private static Map<String, String> buildParameters(Configuration config) {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (Map.Entry<String, String> entry : config) {
          builder.put(entry);
        }
        return builder.build();
    }
    
}
