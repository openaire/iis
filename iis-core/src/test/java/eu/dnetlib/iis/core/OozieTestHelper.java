package eu.dnetlib.iis.core;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

/**
 * Class containing oozie related helper methods used for tests
 * 
 * @author madryk
 *
 */
class OozieTestHelper {

	private OozieClient oozieClient;
	
	
	public OozieTestHelper(OozieClient oozieClient) {
		this.oozieClient = oozieClient;
	}
	
	/**
	 * Returns properties of oozie job with provided id
	 */
	public Properties fetchJobProperties(String jobId) {
		Properties jobProperties = new Properties();
		
		try {
			WorkflowJob jobInfo = oozieClient.getJobInfo(jobId);
			String jobConfigurationString = jobInfo.getConf();
			
			SAXBuilder builder = new SAXBuilder();
			Document doc = builder.build(new StringReader(jobConfigurationString));
			
			for (Object o : doc.getRootElement().getChildren()) {
				Element propertyElement = (Element) o;
				String propertyName = propertyElement.getChildText("name");
				String propertyValue = propertyElement.getChildText("value");
				
				jobProperties.put(propertyName, propertyValue);
				
			}
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		return jobProperties;
	}
	
	/**
	 * Returns oozie job id based on log file
	 */
	public static String readJobIdFromLogFile(File runOozieJobLogFile) {
		
		String jobId;
		try {
			jobId = FileUtils.readFileToString(runOozieJobLogFile);
		} catch (IOException e) {
			throw new RuntimeException("Unable to read run oozie job log file", e);
		}
		Pattern pattern = Pattern.compile("^job: (\\S*)$", Pattern.MULTILINE);
		Matcher matcher = pattern.matcher(jobId);
		matcher.find();
		jobId = matcher.group(1);
		
		return jobId;
	}
	
}
