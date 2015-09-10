package eu.dnetlib.iis.mainworkflows.importer.acm;

import java.io.InputStream;
import java.security.InvalidParameterException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.core.java.PortBindings;
import eu.dnetlib.iis.core.java.Process;
import eu.dnetlib.iis.core.java.io.DataStore;
import eu.dnetlib.iis.core.java.io.FileSystemPath;
import eu.dnetlib.iis.core.java.porttype.AvroPortType;
import eu.dnetlib.iis.core.java.porttype.PortType;
import eu.dnetlib.iis.importer.dataset.DataFileRecordReceiver;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;

/**
 * Process module importing ACM records from xml dump
 * and writing output to avro datastore.
 * @author mhorst
 *
 */
public class AcmXmlImporter implements Process {
	
	private static final String PORT_OUT_DOCUMENT_METADATA = "document_metadata";
	
	public static final String PARAM_ACM_XML_DUMP_PATH = "import.acm.xmldump.path";

	private static final Map<String, PortType> outputPorts = new HashMap<String, PortType>();
	
	{
		outputPorts.put(PORT_OUT_DOCUMENT_METADATA, 
				new AvroPortType(ExtractedDocumentMetadata.SCHEMA$));
	}
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return Collections.emptyMap();
	}

	@Override
	public Map<String, PortType> getOutputPorts() {
		return outputPorts;
	}

	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		if (parameters.containsKey(PARAM_ACM_XML_DUMP_PATH)) {
			DataFileWriter<ExtractedDocumentMetadata> datasetRefWriter = null;
			try {
				datasetRefWriter = DataStore.create(
						new FileSystemPath(fs, portBindings.getOutput().get(PORT_OUT_DOCUMENT_METADATA)), 
						ExtractedDocumentMetadata.SCHEMA$);
				processNode(fs, 
						new Path(parameters.get(PARAM_ACM_XML_DUMP_PATH)),
						datasetRefWriter);
			} finally {
				if (datasetRefWriter!=null) {
					datasetRefWriter.close();	
				}	
			}		
		} else {
			throw new InvalidParameterException("required parameter '" + 
					PARAM_ACM_XML_DUMP_PATH + "' is missing!");
		}
	}

	protected void processNode(FileSystem fs, Path currentPath,
			DataFileWriter<ExtractedDocumentMetadata> datasetRefWriter) throws Exception {
		if (fs.isDirectory(currentPath)) {
			for (FileStatus fileStatus : fs.listStatus(currentPath)) {
				processNode(fs, fileStatus.getPath(), 
						datasetRefWriter);
			}
		} else {
			InputStream inputStream = null;
			SAXParser saxParser = null;
			try {
				saxParser = SAXParserFactory.newInstance().newSAXParser();
				saxParser.parse(inputStream = fs.open(
						currentPath),
						new AcmDumpXmlHandler( 
								new DataFileRecordReceiver<ExtractedDocumentMetadata>(datasetRefWriter)));	
			} finally {
				if (inputStream!=null) {
					inputStream.close();
				}	
			}
		}
	}
	
}
