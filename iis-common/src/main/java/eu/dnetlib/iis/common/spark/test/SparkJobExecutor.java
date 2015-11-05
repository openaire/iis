package eu.dnetlib.iis.common.spark.test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map.Entry;


/**
 * Executor of {@link SparkJob}s
 * 
 * @author Łukasz Dumiszewski
 */

public class SparkJobExecutor {

    
    //------------------------ LOGIC --------------------------
    
    
    /** 
     * Executes the given {@link SparkJob}: <br/><br/>
     * <ol>
     * <li>Sets {@link SparkJob#getJobProperties()} as system properties (see: {@link System#getProperties()})</li>
     * <li>Invokes {@link SparkJob#getMainClass()} with arguments from {@link SparkJob#getArgs()}</li>
     * </ol>
     * 
     * */
    public void execute(SparkJob sparkJob) {
        
        setSystemProperties(sparkJob);
        
        runJob(sparkJob);
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void runJob(SparkJob sparkJob) {
        Class<?> mainClass = sparkJob.getMainClass();
        
        try {
            
            Method method = mainClass.getMethod("main", String[].class);
            method.invoke(null, new Object[] {sparkJob.getArgs()});
            
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            
            throw new RuntimeException(e);
        }
        
    }
    
    private void setSystemProperties(SparkJob sparkJob) {
        for (Entry<String, String> prop : sparkJob.getJobProperties().entrySet()) {
            System.setProperty(prop.getKey(), prop.getValue());
        }
    }
    
}
