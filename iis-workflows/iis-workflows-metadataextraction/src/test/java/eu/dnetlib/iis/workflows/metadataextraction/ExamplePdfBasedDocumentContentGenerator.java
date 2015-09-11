package eu.dnetlib.iis.workflows.metadataextraction;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.file.DataFileWriter;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import eu.dnetlib.iis.core.java.PortBindings;
import eu.dnetlib.iis.core.java.io.DataStore;
import eu.dnetlib.iis.core.java.io.FileSystemPath;
import eu.dnetlib.iis.core.java.porttype.AvroPortType;
import eu.dnetlib.iis.core.java.porttype.PortType;
import eu.dnetlib.iis.importer.schemas.DocumentContent;

/**
 * Generates a directory with a few example avro files containing PDFs.
 * 
 * @author Dominika Tkaczyk
 *
 */
public class ExamplePdfBasedDocumentContentGenerator implements eu.dnetlib.iis.core.java.Process {
	
	private static final String PORT_OUT_DOC_CONTENT = "doc_content";
	private static final String PARAM_PDF_SOURCE_DIR = "pdfs_resource_dir";
	
	private static final Map<String, PortType> outputPorts = new HashMap<String, PortType>(); 
	
	{
		outputPorts.put(PORT_OUT_DOC_CONTENT, 
				new AvroPortType(DocumentContent.SCHEMA$));
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
			
        DataFileWriter<DocumentContent> writer = DataStore.create(
				new FileSystemPath(fs, portBindings.getOutput().get(PORT_OUT_DOC_CONTENT)), 
				DocumentContent.SCHEMA$);
        
        int id = 0;
        for (InputStream is : StandardPDFExamples.getFilesFromResources(parameters.get(PARAM_PDF_SOURCE_DIR))) {
            DocumentContent.Builder docContentBuilder = DocumentContent.newBuilder();
            docContentBuilder.setId("id" + (id++));
            try {
                docContentBuilder.setPdf(ByteBuffer.wrap(IOUtils.toByteArray(is)));
            } finally {
                is.close();
            }
            writer.append(docContentBuilder.build());	
        }				
        	
	}

}
