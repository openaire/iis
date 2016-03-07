package eu.dnetlib.iis.common;

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
	 * Writes properties to file
	 */
	public static void writePropertiesToFile(Properties properties, File file) throws IOException {
		Writer writer = null;

		try {
			writer = new FileWriter(file);
			properties.store(writer, null);
			writer.close();
		} finally {
			IOUtils.closeQuietly(writer);
		}
	}
}
