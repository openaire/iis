package eu.dnetlib.maven.plugin.oozie;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.util.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Read oozie job identifier from log file.
 * @author mhorst
 *
 * @goal read-job-id
 */
public class ReadJobId extends AbstractMojo {
	

    public static final String PROPERTY_NAME_LOG_FILE_LOCATION = "logFileLocation";
    
	public static final String PROPERTY_NAME_OOZIE_JOB_ID = "oozieJobId";
	
	public static final String OOZIE_JOB_LOG_LINE_TOKEN_SEPARATOR = ":";
	
	public static final String OOZIE_JOB_LOG_LINE_PREFIX = "job" + OOZIE_JOB_LOG_LINE_TOKEN_SEPARATOR;
	
	/**
	 * @parameter expression="${properties.logFileLocation}"
	 */
	private String logFileLocation;
	
	/** @parameter default-value="${project}" */
	private MavenProject project;
	
    @Override
    @SuppressFBWarnings({"NP_UNWRITTEN_FIELD","UWF_UNWRITTEN_FIELD"})
    public void execute() throws MojoExecutionException, MojoFailureException {
		String extractedOozieJobId = extractOozieJobId(logFileLocation);
		if (extractedOozieJobId!=null) {
			project.getProperties().setProperty(PROPERTY_NAME_OOZIE_JOB_ID, 
    				extractedOozieJobId);	
			System.out.println("extracted job identifier: " + extractedOozieJobId);
		} else {
			throw new MojoFailureException("unable to extract oozie job identifier from log file: " + 
					logFileLocation);
		}
    }
    
    /**
     * Extracts oozie job identifier from log file
     * @param logFileLocation
     * @return extracted oozie job identifier
     * @throws MojoExecutionException 
     */
    private String extractOozieJobId(String logFileLocation) throws MojoExecutionException {
    	try {
    		BufferedReader reader = new BufferedReader(new InputStreamReader(
        			new FileInputStream(new File(logFileLocation)), "utf8"));
    		String line;
    		try {
    			while ((line = reader.readLine())!=null) {
    				String trimmedLine = line.trim();
        			if (trimmedLine.startsWith(OOZIE_JOB_LOG_LINE_PREFIX)) {
        				String[] split = StringUtils.split(trimmedLine, OOZIE_JOB_LOG_LINE_TOKEN_SEPARATOR);
        				if (split.length == 2) {
        					return split[1].trim();
        				}
        			}
        		}
    			return null;
    		} finally {
    			reader.close();	
    		}
    	} catch (FileNotFoundException e) {
			throw new MojoExecutionException("unable to read log file: " + 
					logFileLocation, e);
    	} catch (IOException e) {
    		throw new MojoExecutionException("error occurred when reading log file: " + 
					logFileLocation, e);
		}
    }

}
