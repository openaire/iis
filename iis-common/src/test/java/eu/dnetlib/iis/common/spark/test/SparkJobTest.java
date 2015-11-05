package eu.dnetlib.iis.common.spark.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import eu.dnetlib.iis.common.spark.test.SparkJob;

/**
 * @author Łukasz Dumiszewski
 */

public class SparkJobTest {

    
    private SparkJob sparkJob = new SparkJob();
    
    
    
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void newSparkJob() {
        
        
        // assert
        
        assertNotNull(sparkJob.getArgs());
        assertEquals(0, sparkJob.getArgs().length);
        assertNull(sparkJob.getMainClass());
        assertNotNull(sparkJob.getJobProperties());
        assertEquals(2, sparkJob.getJobProperties().size());
        assertEquals("local", sparkJob.getJobProperties().get(SparkJob.SPARK_MASTER_KEY));
        assertEquals("Some App", sparkJob.getJobProperties().get(SparkJob.SPARK_APP_NAME_KEY));
        
    }
    
    
    @Test
    public void setMaster() {
        
        // execute
        
        sparkJob.setMaster("local[2]");
        
        // assert
        
        assertEquals("local[2]", sparkJob.getJobProperties().get(SparkJob.SPARK_MASTER_KEY));
        
    }
    
    @Test
    public void setAppName() {
        
        // execute
        
        sparkJob.setAppName("My Great App");
        
        // assert
        
        assertEquals("My Great App", sparkJob.getJobProperties().get(SparkJob.SPARK_APP_NAME_KEY));
        
    }
    
    
    @Test(expected=IllegalArgumentException.class)
    public void setMainClass_NoMainMethod() {
        
        // execute
        
        sparkJob.setMainClass(this.getClass());
        
        
    }
    
    @Test
    public void setMainClass_MainMethod() {
    
        // execute
        
        sparkJob.setMainClass(MainClass.class);
        
        // assert
        
        assertEquals(MainClass.class, sparkJob.getMainClass());
        
    }

    
    @Test
    public void addArg() {
    
        // execute
        
        sparkJob.addArg("argName", "argValue");
        sparkJob.addArg("argName2", "argValue2");
        
        // assert
        
        assertEquals(2, sparkJob.getArgs().length);
        assertEquals("argName=argValue", sparkJob.getArgs()[0]);
        assertEquals("argName2=argValue2", sparkJob.getArgs()[1]);
    }


    @Test
    public void setSeparator() {
    
        // execute
        
        sparkJob.setArgNameValueSeparator(" ");
        sparkJob.addArg("argName", "argValue");
        sparkJob.addArg("argName2", "argValue2");
        
        
        // assert
        
        assertEquals(" ", sparkJob.getArgNameValueSeparator());
        assertEquals(2, sparkJob.getArgs().length);
        assertEquals("argName argValue", sparkJob.getArgs()[0]);
        assertEquals("argName2 argValue2", sparkJob.getArgs()[1]);
    }

    
    
    //------------------------ PRIVATE --------------------------
    
    
    private static class MainClass {
        
        @SuppressWarnings("unused")
        public static void main(String[] args) {
            
        }
    }
    
    
    
}
