package eu.dnetlib.iis.wf.importer.facade;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_FACADE_FACTORY_CLASS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author mhorst
 *
 */
public class ServiceFacadeUtilsTest {

    
    // ------------------------ TESTS --------------------------
    
    @Test
    public void instantiate_no_factory_classname() {
        
        // given
        Map<String, String> parameters = Maps.newHashMap();
        
        // execute
        assertThrows(ServiceFacadeException.class, () -> ServiceFacadeUtils.instantiate(parameters));
    }
    
    @Test
    public void instantiate_invalid_factory_classname() {
        
        // given
        Map<String, String> parameters = Maps.newHashMap();
        parameters.put(IMPORT_FACADE_FACTORY_CLASS, "non.existing.Factory");
        
        // execute
        assertThrows(ServiceFacadeException.class, () -> ServiceFacadeUtils.instantiate(parameters));
    }

    @Test
    public void instantiate_invalid_factory() {
        
        // given
        Map<String, String> parameters = Maps.newHashMap();
        parameters.put(IMPORT_FACADE_FACTORY_CLASS, "java.util.Map");
        
        // execute
        assertThrows(ServiceFacadeException.class, () -> ServiceFacadeUtils.instantiate(parameters));
    }
    
    @Test
    public void instantiate_factory_without_noarg_constructor() {
        
        // given
        Map<String, String> parameters = Maps.newHashMap();
        parameters.put(IMPORT_FACADE_FACTORY_CLASS, "eu.dnetlib.iis.wf.importer.facade.ServiceFacadeUtilsTest$FactoryWithoutNoArgConstructor");
        
        // execute
        assertThrows(ServiceFacadeException.class, () -> ServiceFacadeUtils.instantiate(parameters));
    }
    
    @Test
    public void instantiate() throws Exception {
        
        // given
        Map<String, String> parameters = Maps.newHashMap();
        parameters.put(IMPORT_FACADE_FACTORY_CLASS, "eu.dnetlib.iis.wf.importer.facade.ServiceFacadeUtilsTest$StringFactory");
        
        // execute
        String instantiated = ServiceFacadeUtils.instantiate(parameters);
        
        // assert
        assertEquals(instantiated, StringFactory.class.getName());
    }
    
    
    // ------------------------ INNER CLASSES --------------------------
    
    @SuppressWarnings("unused")
    private static class FactoryWithoutNoArgConstructor implements ServiceFacadeFactory<String> {

        public FactoryWithoutNoArgConstructor(String param) {
            // does nothing
        }
        
        @Override
        public String instantiate(Map<String, String> parameters) {
            return this.getClass().getName();
        }
        
    }
    
    private static class StringFactory implements ServiceFacadeFactory<String> {

        @SuppressWarnings("unused")
        public StringFactory() {
            //does nothing
        }
        
        @Override
        public String instantiate(Map<String, String> parameters) {
            return this.getClass().getName();
        }
        
    }
    
}
