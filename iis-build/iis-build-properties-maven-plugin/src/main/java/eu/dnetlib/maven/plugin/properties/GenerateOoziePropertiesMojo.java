package eu.dnetlib.maven.plugin.properties;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

/**
 * Generates oozie properties which were not provided from commandline.
 * @author mhorst
 *
 * @goal generate-properties
 */
public class GenerateOoziePropertiesMojo extends AbstractMojo {

	public static final String PROPERTY_NAME_WF_SOURCE_DIR = "workflow.source.dir";
	public static final String PROPERTY_NAME_SANDBOX_NAME = "sandboxName";
	
	private final String[] limiters = {"iis", "dnetlib", "eu"};
	
    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
    	if (System.getProperties().containsKey(PROPERTY_NAME_WF_SOURCE_DIR) &&
    			!System.getProperties().containsKey(PROPERTY_NAME_SANDBOX_NAME)) {
    		String generatedSandboxName = generateSandboxName(System.getProperties().getProperty(
					PROPERTY_NAME_WF_SOURCE_DIR));
    		if (generatedSandboxName!=null) {
    			System.getProperties().setProperty(PROPERTY_NAME_SANDBOX_NAME, 
        				generatedSandboxName);	
    		} else {
    			System.out.println("unable to generate sandbox name from path: " + 
    					System.getProperties().getProperty(PROPERTY_NAME_WF_SOURCE_DIR));
    		}
    	}
    }
    
    /**
     * Generates sandbox name from workflow source directory.
     * @param wfSourceDir
     * @return generated sandbox name
     */
    private String generateSandboxName(String wfSourceDir) {
//    	utilize all dir names until finding one of the limiters 
    	List<String> sandboxNameParts = new ArrayList<String>(); 
    	String[] tokens = StringUtils.split(wfSourceDir, File.separatorChar);
    	ArrayUtils.reverse(tokens);
    	if (tokens.length>0) {
    		for (String token : tokens) {
    			for (String limiter : limiters) {
    				if (limiter.equals(token)) {
    					return sandboxNameParts.size()>0?
    							StringUtils.join(sandboxNameParts.toArray()):null;
    				}
    			}
    			if (sandboxNameParts.size()>0) {
    				sandboxNameParts.add(0, File.separator);
    			}
    			sandboxNameParts.add(0, token);
    		}
			return StringUtils.join(sandboxNameParts.toArray());	
    	} else {
    		return null;
    	}
    }

}
