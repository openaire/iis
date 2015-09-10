package eu.dnetlib.iis.importer.content;

import static eu.dnetlib.iis.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_CONNECTION_TIMEOUT;
import static eu.dnetlib.iis.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_READ_TIMEOUT;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import eu.dnetlib.iis.core.java.PortBindings;
import eu.dnetlib.iis.core.java.io.CloseableIterator;
import eu.dnetlib.iis.core.java.io.DataStore;
import eu.dnetlib.iis.core.java.io.FileSystemPath;
import eu.dnetlib.iis.core.java.porttype.AvroPortType;
import eu.dnetlib.iis.core.java.porttype.PortType;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;

/**
 * Content verifier process module.
 * @author mhorst
 *
 */
public class DocumentTextUrlBasedImporterProcess implements eu.dnetlib.iis.core.java.Process {

	private final Logger log = Logger.getLogger(DocumentTextUrlBasedImporterProcess.class);
	
	private final static String contentUrlPort = "content_url";
	
	private final static String textPort = "text";
	
	
	/**
	 * Connection timeout.
	 */
	private int connectionTimeout;
	
	/**
	 * Read timeout.
	 */
	private int readTimeout;
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return createInputPorts();
	}
	
	@Override
	public Map<String, PortType> getOutputPorts() {
		return createOutputPorts();
	}

	private static HashMap<String, PortType> createInputPorts(){
		HashMap<String, PortType> inputPorts = 
				new HashMap<String, PortType>();
		inputPorts.put(contentUrlPort, 
				new AvroPortType(DocumentContentUrl.SCHEMA$));
		return inputPorts;
	}
	
	private static HashMap<String, PortType> createOutputPorts(){
		HashMap<String, PortType> outputPorts = 
				new HashMap<String, PortType>();
		outputPorts.put(textPort, 
				new AvroPortType(DocumentText.SCHEMA$));
		return outputPorts;	
	}
	
	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters)	throws IOException{
		
		readTimeout = parameters.containsKey(
				IMPORT_CONTENT_READ_TIMEOUT)?
						Integer.valueOf(parameters.get(IMPORT_CONTENT_READ_TIMEOUT)):
							conf.getInt(
									IMPORT_CONTENT_READ_TIMEOUT, 60000);
		connectionTimeout = parameters.containsKey(
				IMPORT_CONTENT_CONNECTION_TIMEOUT)?
						Integer.valueOf(parameters.get(IMPORT_CONTENT_CONNECTION_TIMEOUT)):
							conf.getInt(
									IMPORT_CONTENT_CONNECTION_TIMEOUT, 60000);
		
		Map<String, Path> input = portBindings.getInput();
		Map<String, Path> output = portBindings.getOutput();
		FileSystem fs = FileSystem.get(conf);
		CloseableIterator<DocumentContentUrl> contentIt = DataStore.getReader(
				new FileSystemPath(fs, input.get(contentUrlPort)));
		
		
		DataFileWriter<DocumentText> contentWriter = DataStore.create(
				new FileSystemPath(fs, output.get(textPort)),  
				DocumentText.SCHEMA$);
		try {
			int count = 0;
			long timeCheck = System.currentTimeMillis();
			while (contentIt.hasNext()) {
				DocumentContentUrl docUrl = contentIt.next();
				long startTimeContent = System.currentTimeMillis();
				byte[] textContent = ObjectStoreContentProviderUtils.getContentFromURL(
						docUrl.getUrl().toString(), connectionTimeout, readTimeout);
				log.warn("text content retrieval for id: " + docUrl.getId() + 
						" and location: " + docUrl.getUrl() + " took: " +
						(System.currentTimeMillis()-startTimeContent) + " ms, got text content: " +
						(textContent!=null && textContent.length>0));
				if (count%10000==0) {
					log.warn("retrived " + count + " records, last 10000 batch in " +
							((System.currentTimeMillis()-timeCheck)/1000) + " secs");
					timeCheck = System.currentTimeMillis();
				}
				DocumentText.Builder documentTextBuilder = DocumentText.newBuilder();
				documentTextBuilder.setId(docUrl.getId());
				if (textContent!=null) {
					documentTextBuilder.setText(new String(textContent, 
							ObjectStoreContentProviderUtils.defaultEncoding));
				}
				contentWriter.append(documentTextBuilder.build());
//				flushing every time
				contentWriter.flush();
				count++;
			}
		} finally {
			contentIt.close();
			contentWriter.close();
		}
	}
	
}
