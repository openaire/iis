package eu.dnetlib.iis.common.test.spark;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.springframework.util.ReflectionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * 
 * Spark job definition. <br/>
 * Contains all information needed to start a spark job locally: a class with job code, arguments to its main method and
 * spark job properties e.g. spark.master.
 * 
 * 
 * @author Łukasz Dumiszewski
 */

public class SparkJob {

    private Map<String, String> jobProperties = new HashMap<>();
    
    private Map<String, String> mainClassArgs = new HashMap<>();
   
    private Class<?> mainClass;
    
    private String argNameValueSeparator = "=";
    
    public static final String SPARK_MASTER_KEY = "spark.master";
    public static final String SPARK_APP_NAME_KEY = "spark.app.name";
    
    private static final String SPARK_MASTER_DEFAULT_VALUE = "local";
    private static final String SPARK_APP_DEFAULT_VALUE = "Some App";
    
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    
    SparkJob() {

        setMaster(SPARK_MASTER_DEFAULT_VALUE);
        
        setAppName(SPARK_APP_DEFAULT_VALUE);
    
    }  
    
    
    
    //------------------------ GETTERS --------------------------

    /**
     * Class with spark job code in the main method.
     */
    public Class<?> getMainClass() {
        return this.mainClass;
    }
    
    /**
     * Arguments to main method of {@link #getMainClass()}
     */
    public String[] getArgs() {
        List<String> arguments = new ArrayList<>();
        for (Entry<String, String> entry : mainClassArgs.entrySet()) {
            arguments.add(entry.getKey() + argNameValueSeparator + entry.getValue());
        }
        return arguments.toArray(new String[]{});
    }

    /**
     * Spark job properties map of whose keys are e.g. spark.master or spark.app.name
     */
    public Map<String, String> getJobProperties() {
        return ImmutableMap.copyOf(jobProperties);
    }
    
    /**
     * Separator with which the names and values of {@link #getArgs()} will be separated. Defaults to '='.
     * <br/><br/>
     * Let's say you have not changed the default separator (=) and you have added an argument ({@link #addArg(String, String)}) with 
     * name="-inputPath" and value="/temp/xyz". Then the argument will be among those returned by {@link #getArgs()} and 
     * will be equal to: "-inputPath=/temp/xyz".
     */
    public String getArgNameValueSeparator() {
        return argNameValueSeparator;
    }

    
    //------------------------ LOGIC --------------------------

    /**
     * Adds an argument that will be passed to the main method of {@link #getMainClass()} during execution.
     */
    public void addArg(String argName, String argValue) {
        mainClassArgs.put(argName, argValue);
    }

    /**
     * Adds a spark job property. 
     * 
     * @see #setMaster(String)
     * @see #setAppName(String)
     */
    public void addJobProperty(String sparkPropertyName, String sparkPropertyValue) {
        this.jobProperties.put(sparkPropertyName, sparkPropertyValue);
    }

    //------------------------ SETTERS --------------------------
    
    /**
     * Sets the {@value #SPARK_MASTER_KEY} property of spark job. Defaults to {@value #SPARK_MASTER_DEFAULT_VALUE}. 
     * <br/><br/>
     * It's a convenient alternative of {@link #addJobProperty(String, String)} for adding {@value #SPARK_MASTER_KEY} property
     */
    void setMaster(String sparkMaster) {
        Preconditions.checkArgument(StringUtils.isNotBlank(sparkMaster));
        jobProperties.put(SPARK_MASTER_KEY, sparkMaster);
    }
    
    /**
     * Sets the {@value #SPARK_APP_NAME_KEY} property of spark job. Defaults to {@value #SPARK_APP_DEFAULT_VALUE}.
     * <br/><br/>
     * It's a convenient alternative of {@link #addJobProperty(String, String)} for adding {@value #SPARK_APP_NAME_KEY} property
     */
    void setAppName(String sparkAppName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(sparkAppName));
        jobProperties.put(SPARK_APP_NAME_KEY, sparkAppName);
    }
    
    /**
     * See {@link #getMainClass()}
     */
    void setMainClass(Class<?> mainClass) {
        checkHasMainMethod(mainClass);
        this.mainClass = mainClass;
    }
    
    /**
     * See {@link #getArgNameValueSeparator()}
     */
    void setArgNameValueSeparator(String argNameValueSeparator) {
        Preconditions.checkNotNull(argNameValueSeparator);
        this.argNameValueSeparator = argNameValueSeparator;
    }
    
    
   
    //------------------------ PRIVATE --------------------------
    
    private void checkHasMainMethod(Class<?> mainClass) {
        Method mainMethod = ReflectionUtils.findMethod(mainClass, "main", String[].class);
        Preconditions.checkArgument(mainMethod != null 
                && Modifier.isPublic(mainMethod.getModifiers()) 
                && Modifier.isStatic(mainMethod.getModifiers()), "the passed class has no main method");
    }


 
   
    
    
    
}
