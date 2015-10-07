package eu.dnetlib.iis.core;

import java.io.StringReader;
import java.util.Properties;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

/**
 * Fetcher of oozie job properties 
 * 
 * @author madryk
 *
 */
class OozieJobPropertiesFetcher {

	private OozieClient oozieClient;
	
	
	//------------------------ CONSTRUCTORS --------------------------
	
	public OozieJobPropertiesFetcher(OozieClient oozieClient) {
		this.oozieClient = oozieClient;
	}
	
	
	//------------------------ LOGIC --------------------------
	
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
	
}
