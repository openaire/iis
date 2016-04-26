package eu.dnetlib.iis.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Runner of test workflows using maven
 * 
 * @author madryk
 *
 */
public class MavenTestWorkflowRunner {

    private final static Logger log = LoggerFactory.getLogger(MavenTestWorkflowRunner.class);
    
    
    private final static String WORKFLOW_SOURCE_DIR_KEY = "workflow.source.dir";
    
    private final static String MAVEN_TEST_WORKFLOW_PHASE = "clean package";
    
    private final static String MAVEN_TEST_WORKFLOW_PROFILE = "attach-test-resources,oozie-package,deploy,run";
    
    
    private String mavenExecutable;
    
    private String outputDirName;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    /**
     * Default constructor
     */
    public MavenTestWorkflowRunner(String mavenExecutable, String outputDirName) {
        Preconditions.checkNotNull(mavenExecutable);
        
        this.mavenExecutable = mavenExecutable;
        this.outputDirName = outputDirName;
    }
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Runs test workflow on cluster using maven
     * 
     * @param workflowPath - path to workflow
     * @param connectionPropertiesFilePath - properties of cluster
     * @return exit status of maven process
     */
    public int runTestWorkflow(String workflowPath, String connectionPropertiesFilePath) {
        
        Process p = runMavenTestWorkflow(workflowPath, connectionPropertiesFilePath);
        logMavenOutput(p);
        
        return checkMavenExitStatus(p);
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private Process runMavenTestWorkflow(String workflowSource, String connectionPropertiesFilePath) {
        
        Process p;
        try {
            StringBuilder mvnCommandBuilder = new StringBuilder();
            
            mvnCommandBuilder.append(mavenExecutable + " " + MAVEN_TEST_WORKFLOW_PHASE + " -DskipTests ");
            mvnCommandBuilder.append(" -P" + MAVEN_TEST_WORKFLOW_PROFILE);
            mvnCommandBuilder.append(" -D" + WORKFLOW_SOURCE_DIR_KEY + "=" + workflowSource);
            if (outputDirName != null) {
                mvnCommandBuilder.append(" -D" + "output.dir.name" + "=" + outputDirName);
            }
            mvnCommandBuilder.append(" -DiisConnectionProperties=" + connectionPropertiesFilePath);
            
            p = Runtime.getRuntime().exec(mvnCommandBuilder.toString());
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        return p;
    }
    
    private void logMavenOutput(Process p) {
        
        BufferedReader stdInput = new BufferedReader(new
                InputStreamReader(p.getInputStream()));
        try {
            String s = null;
            while ((s = stdInput.readLine()) != null) {
                log.info(s);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(stdInput);
        }
        

        BufferedReader stdError = new BufferedReader(new
                InputStreamReader(p.getErrorStream()));
        try {
            String s = null;
            while ((s = stdError.readLine()) != null) {
                log.error(s);
            }
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(stdError);
        }
    }
    
    private int checkMavenExitStatus(Process p) {
        try {
            return p.waitFor();
        } catch (InterruptedException e) {
            throw new RuntimeException("Error in waiting for maven process to finish", e);
        }
        
    }
}
