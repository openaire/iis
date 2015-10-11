package eu.dnetlib.iis.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

import com.google.common.base.Preconditions;

/**
 * Reader of properties for integration tests.<br/>
 * It uses file {@literal classpath:integration-test-default.properties} as a source of properties 
 * and allows for overriding them in ${user.home}/.iis/integration-test.properties
 * 
 * @author madryk
 *
 */
public class IntegrationTestPropertiesReader {

	private final static String DEFAULT_PROPERTIES_CLASSPATH = "/integration-test-default.properties";
	
	private final static String DEFAULT_USER_PROPERTIES_PATH = System.getProperty("user.home") + "/.iis/integration-test.properties";
	
	private Properties integrationTestProperties = new Properties();
	
	
	//------------------------ CONSTRUCTORS --------------------------
	
	/**
	 * Default constructor. It handles reading properties from files.
	 */
	public IntegrationTestPropertiesReader() {
	    Properties defaultProperties = readDefaultProperties();
	    Properties userProperties = readUserProperties();
	    
	    integrationTestProperties.putAll(defaultProperties);
	    integrationTestProperties.putAll(userProperties);
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
	
	public String getPropertiesFilePath() {
		String userPropertiesPath = System.getProperty("connectionProperties");
		if (userPropertiesPath == null) {
			return DEFAULT_USER_PROPERTIES_PATH;
		}
		return userPropertiesPath;
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
			File userPropertiesFile = new File(getPropertiesFilePath());

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
	
}
