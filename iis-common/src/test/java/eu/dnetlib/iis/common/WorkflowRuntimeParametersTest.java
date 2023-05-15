package eu.dnetlib.iis.common;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author mhorst
 *
 */
public class WorkflowRuntimeParametersTest {

    Configuration configuration;
    
    @BeforeEach
    public void init() {
        configuration = new Configuration();
    }
    
    @Test
    public void testGetValueOrNullIfNotValidWithValid() throws Exception {
    	// given
    	String value = "some value";
    	
    	// assert
    	assertEquals(value, WorkflowRuntimeParameters.getValueOrNullIfNotValid(value));
    }
    
    @Test
    public void testGetValueOrNullIfNotValidWithInvalid() throws Exception {
    	// assert
    	assertNull(WorkflowRuntimeParameters.getValueOrNullIfNotValid(" "));
    	assertNull(WorkflowRuntimeParameters.getValueOrNullIfNotValid(""));
    	assertNull(WorkflowRuntimeParameters.getValueOrNullIfNotValid(null));
    	assertNull(WorkflowRuntimeParameters.getValueOrNullIfNotValid(WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE));
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
    public void testGetParamValueWithoutFallback() throws Exception {
        // given
        String paramName = "paramName1";
        String paramValue = "paramValue1";
        String paramNameFallback = "paramNameMissing";
        configuration.set(paramName, paramValue);
        
        // execute
        String result = WorkflowRuntimeParameters.getParamValue(
        		paramName, paramNameFallback, configuration);
        
        // assert
        assertEquals(paramValue, result);
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
    
    @Test
    public void testGetParamValueWithUndefinedCheckWithProperParamValue() {
    	// given
        String paramName = "paramName1";
        String paramValue = "paramValue1";
        String paramDefaultValue = "defaultValue";
        Map<String, String> parameters = new HashMap<>();
        parameters.put(paramName, paramValue);
        
        // execute & assert
        assertEquals(paramValue, 
                WorkflowRuntimeParameters.getParamValueWithUndefinedCheck(paramName, paramDefaultValue, parameters));
    }
    
    @Test
    public void testGetParamValueWithUndefinedCheckWithUndefinedParamValue() {
    	// given
        String paramNameMissing = "paramNameMissing";
        String paramDefaultValue = "defaultValue";
        Map<String, String> parameters = new HashMap<>();
        
        // execute & assert
        assertEquals(paramDefaultValue, 
                WorkflowRuntimeParameters.getParamValueWithUndefinedCheck(paramNameMissing, paramDefaultValue, parameters));
    }

}
