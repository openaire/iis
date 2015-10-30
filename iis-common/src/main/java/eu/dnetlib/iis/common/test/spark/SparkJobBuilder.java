package eu.dnetlib.iis.common.test.spark;

import com.google.common.base.Preconditions;

/**
 * 
 * {@link SparkJob} builder.
 * 
 * @author Łukasz Dumiszewski
 */

public class SparkJobBuilder {

    private SparkJob sparkJob;

    
    //------------------------ CONSTRUCTORS --------------------------
    
    private SparkJobBuilder(SparkJob sparkJob) {
        this.sparkJob = sparkJob;
    }
    
    
    //------------------------ LOGIC --------------------------
    
    public static SparkJobBuilder create() {
        return new SparkJobBuilder(new SparkJob());
    }

    /**
     * See {@link SparkJob#setMainClass(Class)}
     */
    public SparkJobBuilder setMainClass(Class<?> mainClass) {
        sparkJob.setMainClass(mainClass);
        return this;
    }
    
    /**
     * See {@link SparkJob#addArg(Class)}
     */
    public SparkJobBuilder addArg(String argName, String argValue) {
        sparkJob.addArg(argName, argValue);
        return this;
    }
    
    /**
     * See {@link SparkJob#addJobProperty(String, String) <br/><br/>
     * 
     * @see #setMaster(String)
     * @see #setAppName(String)
     */
    public SparkJobBuilder addJobProperty(String argName, String argValue) {
        sparkJob.addJobProperty(argName, argValue);
        return this;
    }
    
    /**
     * See {@link SparkJob#setMaster(Class)}
     */
    public SparkJobBuilder setMaster(String sparkMaster) {
        sparkJob.setMaster(sparkMaster);
        return this;
    }
    
    /**
     * See {@link SparkJob#setAppName(Class)}
     */
    public SparkJobBuilder setAppName(String sparkAppName) {
        sparkJob.setAppName(sparkAppName);
        return this;
    }
    
    
    /**
     * See {@link SparkJob#setArgNameValueSeparator(String)}
     */
    public void setArgNameValueSeparator(String separator) {
        sparkJob.setArgNameValueSeparator(separator);
    }
    
    public SparkJob build() {
        Preconditions.checkNotNull(sparkJob.getMainClass(), "main class not set");
        return sparkJob;
    }
}
