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
 * MDStore resource classpath locations should be specified as input parameters 
 * where keys are defined as: mdstore.facade.mock.static.resource.${mdStoreId}.{recordId} 
 * and classpath locations are defined as parameter values.
 * 
 * @author mhorst
 *
 */
public class MockMDStoreFacadeFactory implements MDStoreFacadeFactory {
    
    private static final String STATIC_RESOURCE_PREFIX = "mdstore.facade.mock.static.resource.";
    
    private static final char SEPARATOR_IDS = '.';

    // -------------------- LOGIC -------------------------
    
    @Override
    public MDStoreFacade create(Map<String, String> parameters) {
        return new MockMDStoreFacade(parameters);
    }
    
    /**
     * Builds parameter key indicating record to be returned by mocked implementation.
     */
    public static String buildParameterKey(String mdStoreId, String entityId) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(MockMDStoreFacadeFactory.STATIC_RESOURCE_PREFIX);
        strBuilder.append(mdStoreId);
        strBuilder.append(MockMDStoreFacadeFactory.SEPARATOR_IDS);
        strBuilder.append(entityId);
        return strBuilder.toString();
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
        public String fetchRecord(String mdStoreId, String recordId) throws MDStoreServiceException {
            String parameterKey = STATIC_RESOURCE_PREFIX + mdStoreId + SEPARATOR_IDS + recordId;
            if (runtimeParameters.containsKey(parameterKey)) {
                return readRecord(runtimeParameters.get(parameterKey));
            } else {
                throw new DocumentNotFoundException("unable to find static resource for mdStoreId: " + 
                        mdStoreId + " and recordId: " + recordId + ", no '" + parameterKey + " parameter defined!");
            }
        }
        
        // -------------------- PRIVATE -------------------------
        
        private String readRecord(String resourceLocation) {
            try (InputStream input = MockMDStoreFacadeFactory.class.getResourceAsStream(resourceLocation)) {
                return IOUtils.toString(input, "utf8");
            } catch (IOException e) {
                throw new RuntimeException("Unable to read resource: " + resourceLocation, e);
            }
        }
        
    }

}
