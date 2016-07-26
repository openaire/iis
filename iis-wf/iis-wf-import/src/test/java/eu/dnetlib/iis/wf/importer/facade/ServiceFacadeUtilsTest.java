package eu.dnetlib.iis.wf.importer.facade;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_FACADE_FACTORY_CLASS;
import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Maps;

/**
 * @author mhorst
 *
 */
public class ServiceFacadeUtilsTest {

    
    // ------------------------ TESTS --------------------------
    
    @Test(expected = ServiceFacadeException.class)
    public void instantiate_no_factory_classname() throws Exception {
        
        // given
        Map<String, String> parameters = Maps.newHashMap();
        
        // execute
        ServiceFacadeUtils.instantiate(parameters);
    }
    
    @Test(expected = ServiceFacadeException.class)
    public void instantiate_invalid_factory_classname() throws Exception {
        
        // given
        Map<String, String> parameters = Maps.newHashMap();
        parameters.put(IMPORT_FACADE_FACTORY_CLASS, "non.existing.Factory");
        
        // execute
        ServiceFacadeUtils.instantiate(parameters);
    }

    @Test(expected = ServiceFacadeException.class)
    public void instantiate_invalid_factory() throws Exception {
        
        // given
        Map<String, String> parameters = Maps.newHashMap();
        parameters.put(IMPORT_FACADE_FACTORY_CLASS, "java.util.Map");
        
        // execute
        ServiceFacadeUtils.instantiate(parameters);
    }
    
    @Test(expected = ServiceFacadeException.class)
    public void instantiate_factory_without_noarg_constructor() throws Exception {
        
        // given
        Map<String, String> parameters = Maps.newHashMap();
        parameters.put(IMPORT_FACADE_FACTORY_CLASS, "eu.dnetlib.iis.wf.importer.facade.ServiceFacadeUtilsTest$FactoryWithoutNoArgConstructor");
        
        // execute
        ServiceFacadeUtils.instantiate(parameters);
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
    
    public static class FactoryWithoutNoArgConstructor implements ServiceFacadeFactory<String> {

        public FactoryWithoutNoArgConstructor(String param) {
            // does nothing
        }
        
        @Override
        public String instantiate(Map<String, String> parameters) {
            return this.getClass().getName();
        }
        
    }
    
    public static class StringFactory implements ServiceFacadeFactory<String> {

        @Override
        public String instantiate(Map<String, String> parameters) {
            return this.getClass().getName();
        }
        
    }
    
}
