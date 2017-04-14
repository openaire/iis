package eu.dnetlib.iis.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

/**
 * @author mhorst
 *
 */
public class WorkflowRuntimeParametersTest {

    Configuration configuration;
    
    @Before
    public void init() {
        configuration = new Configuration();
    }
    
    
    @Test
    public void testGetParamValue() throws Exception {
        // given
        String paramName = "paramName1";
        String paramValue = "paramValue1";
        configuration.set(paramName, paramValue);
        
        // execute
        String result = WorkflowRuntimeParameters.getParamValue(paramName, configuration);
        
        // assert
        assertEquals(paramValue, result);
    }

    @Test
    public void testGetParamValueMissing() throws Exception {
        // given
        String paramName = "paramName1";
        
        // execute
        String result = WorkflowRuntimeParameters.getParamValue(paramName, configuration);
        
        // assert
        assertNull(result);
    }
    
    @Test
    public void testGetParamValueBlank() throws Exception {
        // given
        String paramName = "paramName1";
        String paramValue = "";
        configuration.set(paramName, paramValue);
        
        // execute
        String result = WorkflowRuntimeParameters.getParamValue(paramName, configuration);
        
        // assert
        assertNull(result);
    }
    
    @Test
    public void testGetIntegerParamValue() throws Exception {
        // given
        String paramName = "paramName1";
        int paramValue = 12;
        configuration.set(paramName, String.valueOf(paramValue));
        
        // execute
        Integer result = WorkflowRuntimeParameters.getIntegerParamValue(paramName, configuration);
        
        // assert
        assertEquals(paramValue, result.intValue());
    }
    
    @Test
    public void testGetIntegerParamValueMissing() throws Exception {
        // given
        String paramName = "paramName1";
        
        // execute
        Integer result = WorkflowRuntimeParameters.getIntegerParamValue(paramName, configuration);
        
        // assert
        assertNull(result);
    }
    
    @Test
    public void testGetParamValueWithFallback() throws Exception {
        // given
        String paramName = "paramName1";
        String paramValue = "paramValue1";
        String paramNameMissing = "paramNameMissing";
        configuration.set(paramName, paramValue);
        
        // execute
        String result = WorkflowRuntimeParameters.getParamValue(
                paramNameMissing, paramName, configuration);
        
        // assert
        assertEquals(paramValue, result);
    }
    
    @Test
    public void testGetParamValueWithDefault() throws Exception {
        // given
        String paramName = "paramName1";
        String paramValue = "paramValue1";
        String paramNameMissing = "paramNameMissing";
        String paramDefaultValue = "defaultValue";
        Map<String, String> parameters = new HashMap<>();
        parameters.put(paramName, paramValue);
        
        // execute & assert
        assertEquals(paramValue, 
                WorkflowRuntimeParameters.getParamValue(paramName, paramDefaultValue, parameters));
        assertEquals(paramDefaultValue, 
                WorkflowRuntimeParameters.getParamValue(paramNameMissing, paramDefaultValue, parameters));
    }

}
