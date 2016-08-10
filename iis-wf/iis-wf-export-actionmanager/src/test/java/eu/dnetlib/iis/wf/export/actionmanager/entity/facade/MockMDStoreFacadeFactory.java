package eu.dnetlib.iis.wf.export.actionmanager.entity.facade;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.data.mdstore.DocumentNotFoundException;
import eu.dnetlib.data.mdstore.MDStoreServiceException;

/**
 * {@link MDStoreFacadeFactory} mock implementation.
 * 
 * MDStore resource classpath locations should specified as input parameters 
 * where keys are defined as: mock.mdstore.static.resource.${mdStoreId}.{recordId} 
 * and classpath locations are defined as parameter values.
 * 
 * @author mhorst
 *
 */
public class MockMDStoreFacadeFactory implements MDStoreFacadeFactory {
    
    private static String STATIC_RESOURCE_PREFIX = "mdstore.facade.mock.static.resource.";

    // -------------------- LOGIC -------------------------
    
    @Override
    public MDStoreFacade instantiate(Map<String, String> parameters) {
        return new MockMDStoreFacade(parameters);
    }
    
    // -------------------- INNER CLASS -------------------------
    
    private class MockMDStoreFacade implements MDStoreFacade {

        
        private final Map<String, String> runtimeParameters;
        
        
        // -------------------- CONSTRUCTORS -------------------------

        private MockMDStoreFacade(Map<String, String> parameters) {
            this.runtimeParameters = Preconditions.checkNotNull(parameters);
        }
        
        // -------------------- LOGIC -------------------------
        
        @Override
        public String deliverRecord(String mdStoreId, String recordId) throws MDStoreServiceException {
            String parameterKey = STATIC_RESOURCE_PREFIX + mdStoreId + '.' + recordId;
            if (runtimeParameters.containsKey(parameterKey)) {
                return obtainResource(runtimeParameters.get(parameterKey));
            } else {
                throw new DocumentNotFoundException("unable to find static resource for mdStoreId" + 
                        mdStoreId + " and recordId " + recordId + ", no '" + parameterKey + " parameter defined!");
            }
        }
        
        // -------------------- PRIVATE -------------------------
        
        private String obtainResource(String resourceLocation) {
            try (InputStream input = MockMDStoreFacadeFactory.class.getResourceAsStream(resourceLocation)) {
                return IOUtils.toString(input);
            } catch (IOException e) {
                throw new RuntimeException("Unable to read resource: " + resourceLocation, e);
            }
        }
        
    }

}
