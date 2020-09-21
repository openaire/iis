package eu.dnetlib.iis.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

/**
 * Reader of properties for integration tests.<br/>
 * It uses file {@literal classpath:integration-test-default.properties} as a source of properties 
 * and allows for overriding them with user properties.<br/>
 * User properties should be stored in file defined in ${iisConnectionProperties} system property.
 * If ${iisConnectionProperties} is not present, then ${user.home}/.iis/integration-test.properties
 * will be used.<br/>
 * 
 * @author madryk
 *
 */
public class IntegrationTestPropertiesReader {

	private final static String DEFAULT_PROPERTIES_CLASSPATH = "/integration-test-default.properties";
	
	private final static String DEFAULT_USER_PROPERTIES_PATH = System.getProperty("user.home") + "/.iis/integration-test.properties";
	
	
	//------------------------ LOGIC --------------------------
	
	/**
	 * Returns all properties
	 */
	public Properties readProperties() {
		Properties defaultProperties = readDefaultProperties();
		Properties userProperties = readUserProperties();

		Properties integrationTestProperties = new Properties();
		integrationTestProperties.putAll(defaultProperties);
		integrationTestProperties.putAll(userProperties);
		
		return integrationTestProperties;
	}
	
	
	//------------------------ PRIVATE --------------------------
	
	private Properties readDefaultProperties() {
		Properties defaultProperties = new Properties();

		InputStream inputStream = null;
		try {

			inputStream = ClassPathResourceProvider.getResourceInputStream(DEFAULT_PROPERTIES_CLASSPATH);
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
		String userPropertiesPath = System.getProperty("iisConnectionProperties");
		if (userPropertiesPath == null) {
			return DEFAULT_USER_PROPERTIES_PATH;
		}
		return userPropertiesPath;
	}
	
}
