package eu.dnetlib.iis.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

import com.google.common.base.Preconditions;

/**
 * Reader of properties for integration tests.<br/>
 * It uses file {@literal classpath:integration-test-default.properties} as a source of properties 
 * and allows for overriding them with user properties.<br/>
 * User properties should be stored in file defined in ${connectionProperties} system property.
 * If ${connectionProperties} is not present, then ${user.home}/.iis/integration-test.properties
 * will be used.<br/>
 * Merged properties (default and user ones) can be accessed through {@link #getProperty(String)}.
 * Alternative way to access merged properties is through a temporary properties file
 * created by this class - {@link #getPropertiesFilePath()}.<br/>
 * After integration test is completed it is required to execute {@link #clean()} method
 * 
 * @author madryk
 *
 */
public class IntegrationTestPropertiesReader {

	private final static String DEFAULT_PROPERTIES_CLASSPATH = "/integration-test-default.properties";
	
	private final static String DEFAULT_USER_PROPERTIES_PATH = System.getProperty("user.home") + "/.iis/integration-test.properties";
	
	private Properties integrationTestProperties = new Properties();
	
	private File integrationTestPropertiesFile;
	
	
	//------------------------ CONSTRUCTORS --------------------------
	
	/**
	 * Default constructor. It handles reading properties from files.
	 * It also creates temporary file with merged properties.
	 */
	public IntegrationTestPropertiesReader() {
	    Properties defaultProperties = readDefaultProperties();
	    Properties userProperties = readUserProperties();
	    
	    integrationTestProperties.putAll(defaultProperties);
	    integrationTestProperties.putAll(userProperties);
	    
	    createTempPropertiesFile();
	}
	
	
	//------------------------ LOGIC --------------------------
	
	/**
	 * Returns value of property with key provided as argument
	 */
	public String getProperty(String key) {
		Preconditions.checkNotNull(key);
		Preconditions.checkArgument(integrationTestProperties.containsKey(key), "Property '%s' is not defined for integration tests", key);
		
		return integrationTestProperties.getProperty(key);
	}
	
	/**
	 * Returns absolute path to file with properties used in integration tests.
	 */
	public String getPropertiesFilePath() {
		return integrationTestPropertiesFile.getAbsolutePath();
	}
	
	/**
	 * Releases resources used by this class (Removes temporary properties file)
	 */
	public void clean() {
		if (integrationTestPropertiesFile != null) {
			integrationTestPropertiesFile.delete();
		}
	}
	
	
	//------------------------ PRIVATE --------------------------
	
	private Properties readDefaultProperties() {
		Properties defaultProperties = new Properties();

		InputStream inputStream = null;
		try {

			inputStream = IntegrationTestPropertiesReader.class.getResourceAsStream(DEFAULT_PROPERTIES_CLASSPATH);
			defaultProperties.load(inputStream);
			inputStream.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtils.closeQuietly(inputStream);
		}

		return defaultProperties;
	}
	
	private Properties readUserProperties() {
		Properties userProperties = new Properties();

		InputStream inputStream = null;
		try {
			File userPropertiesFile = new File(resolveUserPropertiesFilePath());

			if (!userPropertiesFile.exists()) {
				return userProperties;
			}

			inputStream = new FileInputStream(userPropertiesFile);
			userProperties.load(inputStream);
			inputStream.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtils.closeQuietly(inputStream);
		}

		return userProperties;
	}
	
	private String resolveUserPropertiesFilePath() {
		String userPropertiesPath = System.getProperty("connectionProperties");
		if (userPropertiesPath == null) {
			return DEFAULT_USER_PROPERTIES_PATH;
		}
		return userPropertiesPath;
	}
	
	private void createTempPropertiesFile() {
		Writer writer = null;
		
		try {
	    	integrationTestPropertiesFile = File.createTempFile("iis-integration-test", ".properties");
	    	writer = new FileWriter(integrationTestPropertiesFile);
	    	integrationTestProperties.store(writer, null);
	    	writer.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtils.closeQuietly(writer);
		}
	}
	
}
