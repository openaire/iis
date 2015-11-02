package eu.dnetlib.iis.common.test.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

/**
 * @author Łukasz Dumiszewski
 */

public class SparkJobExecutorTest {

    private SparkJobExecutor sparkJobExecutor = new SparkJobExecutor();
    
    
    
    @Test
    public void execute() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        
        // given
        
        SparkJob sparkJob = Mockito.mock(SparkJob.class);
        String[] args = new String[]{"input=ssss"};
        when(sparkJob.getArgs()).thenReturn(args);
        
        Map<String, String> jobProperties = new HashMap<>();
        jobProperties.put("testPropName", "testPropValue");
        when(sparkJob.getJobProperties()).thenReturn(jobProperties);
        
        Mockito.doReturn(MainClass.class).when(sparkJob).getMainClass();
        
         
        // execute
        
        sparkJobExecutor.execute(sparkJob);
        
        
        // assert
        
        assertTrue(MainClass.argsPassed == args);
        assertNotNull(System.getProperty("testPropName"));
        assertEquals("testPropValue", System.getProperty("testPropName"));
        
        
    }
    
    
    private static class MainClass {
        
        private static String[] argsPassed = null;
        
        @SuppressWarnings("unused")
        public static void main(String[] args) {
            argsPassed = args;
        }
    }
    
}
