package eu.dnetlib.iis.core;

import java.io.StringReader;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.oozie.client.WorkflowJob.Status;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

/**
 * Parser of oozie command line answers
 * 
 * @author madryk
 *
 */
class OozieCmdLineAnswerParser {
	
	private final static Pattern JOB_STATUS_PATTERN = Pattern.compile("^status[ ]*: (\\S*)$", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
	
	
	//------------------------ LOGIC --------------------------
	
	/**
	 * Returns oozie job parsed properties
	 */
	public Properties parseJobProperties(String jobPropertiesString) {
		Properties jobProperties = new Properties();
		
		try {
			
			SAXBuilder builder = new SAXBuilder();
			Document doc = builder.build(new StringReader(jobPropertiesString));
			
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
	 * Returns oozie job status based on job info string
	 */
	public Status readStatusFromJobInfo(String jobInfoString) {
		Status status;
		Matcher matcher = JOB_STATUS_PATTERN.matcher(jobInfoString);
		
		if (matcher.find()) {
			String statusString = matcher.group(1);
			status = Status.valueOf(statusString);
		} else {
			throw new RuntimeException("Unable to find job status in job info string");
		}
		
		return status;
	}
	
}
