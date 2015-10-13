package eu.dnetlib.iis.core;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

/**
 * Utility class for property files
 * 
 * @author krzysztof
 *
 */
public class PropertiesFileUtils {

	/**
	 * Creates property file in system temporary directory
	 */
	public static File createTemporaryPropertiesFile(Properties properties, String filenamePrefix) throws IOException {
		Writer writer = null;
		File temporaryPropertiesFile;

		try {
			temporaryPropertiesFile = File.createTempFile(filenamePrefix, ".properties");
			writer = new FileWriter(temporaryPropertiesFile);
			properties.store(writer, null);
			writer.close();
		} finally {
			IOUtils.closeQuietly(writer);
		}

		return temporaryPropertiesFile;
	}
}
