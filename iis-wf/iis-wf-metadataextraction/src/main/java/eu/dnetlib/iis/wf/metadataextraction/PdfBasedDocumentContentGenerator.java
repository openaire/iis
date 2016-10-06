package eu.dnetlib.iis.wf.metadataextraction;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.file.DataFileWriter;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.Logger;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.importer.schemas.DocumentContent;

/**
 * PDF files based {@link DocumentContent} avro files generator.
 * 
 * @author mhorst
 *
 */
public class PdfBasedDocumentContentGenerator implements eu.dnetlib.iis.common.java.Process {

	public static final String PARAM_HDFS_CONTENT_DIR = "hdfsContentDir";
	
	private static final String PORT_OUT_DOC_CONTENT = "doc_content";
	
	
	private static final Logger log = Logger.getLogger(PdfBasedDocumentContentGenerator.class);
	
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
		RemoteIterator<LocatedFileStatus> filesIt = fs.listFiles(
				new Path(parameters.get(PARAM_HDFS_CONTENT_DIR)), true);
		
		DataFileWriter<DocumentContent> writer = DataStore.create(
				new FileSystemPath(fs, portBindings.getOutput().get(PORT_OUT_DOC_CONTENT)), 
				DocumentContent.SCHEMA$);
		try {
			int idx = 1;
			while (filesIt.hasNext()) {
				LocatedFileStatus fileStatus = filesIt.next();
				if (!fileStatus.isDirectory()) {
					DocumentContent.Builder docContentBuilder = DocumentContent.newBuilder();
					docContentBuilder.setId("" + System.currentTimeMillis() + '-' + idx);
					
					FSDataInputStream inputStream = fs.open(fileStatus.getPath());
					try {
						docContentBuilder.setPdf(ByteBuffer.wrap(
								IOUtils.toByteArray(inputStream)));
					} finally {
						inputStream.close();
					}
					writer.append(docContentBuilder.build());
					idx++;	
				} else {
					log.info("skipping directory:" + fileStatus.getPath().toString());
				}
			}	
		} finally {
			writer.close();
		}
		
	}

}
