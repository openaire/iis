package eu.dnetlib.iis.workflows.referenceextraction.project.importer;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.InvalidParameterException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.file.DataFileWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.ProcessUtils;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.workflows.referenceextraction.shared.importer.SharedImporterUtils;

/**
 * Module importing data from existing CSV lists and producing
 * {@link DocumentToProject} avro datastore.
 * @author mhorst
 *
 */
public class DocumentToProjectImporter implements eu.dnetlib.iis.common.java.Process {

	private final Logger log = Logger.getLogger(this.getClass());
	
	public static final String PARAM_CSV_PATH = "import.project.csv.path";

	private static final String outputPortName = "output";
	
	private static final char separatorChar = '\t';
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return Collections.emptyMap();
	}

	@Override
	public Map<String, PortType> getOutputPorts() {
		Map<String, PortType> output = new HashMap<String, PortType>();
		output.put(outputPortName, 
				new AvroPortType(DocumentToProject.SCHEMA$));
		return output;
	}

	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		String csvPath = ProcessUtils.getParameterValue(
				PARAM_CSV_PATH, conf, parameters);
		if (csvPath!=null && !csvPath.isEmpty()) {
			DataFileWriter<DocumentToProject> writer = null;
			try {
				writer = DataStore.create(
						new FileSystemPath(fs, portBindings.getOutput().get(outputPortName)), 
						DocumentToProject.SCHEMA$);
				processNode(fs, 
						new Path(csvPath),
						writer);
			} finally {
				if (writer!=null) {
					writer.close();	
				}	
			}		
		} else {
			throw new InvalidParameterException("required parameter '" + 
					PARAM_CSV_PATH + "' is missing!");
		}
	}

	protected void processNode(FileSystem fs, Path currentPath,
			DataFileWriter<DocumentToProject> writer) throws Exception {
		if (fs.isDirectory(currentPath)) {
			for (FileStatus fileStatus : fs.listStatus(currentPath)) {
				processNode(fs, fileStatus.getPath(), 
						writer);
			}
		} else {
			InputStream inputStream = null;
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new InputStreamReader(
						inputStream = fs.open(currentPath)));
				String line = null;
				while ((line = reader.readLine()) != null) {
					if (SharedImporterUtils.skipLine(line)) {
						log.warn("skipping line: " + line);
						continue;
					} else {
						String[] split = StringUtils.split(
								line, separatorChar);
						if (split.length>=2) {
							DocumentToProject.Builder builder = DocumentToProject.newBuilder();
							builder.setDocumentId(generateDocumentId(split[0].trim()));
							builder.setProjectId(generateProjectId(split[1].trim()));
//							TODO currently there is no confidence level available in CSV
//							builder.setConfidenceLevel(value);
							writer.append(builder.build());
						} else {
							log.warn("invalid line, unable to process: " + line);
						}
					}
				}
			} finally {
				if (reader!=null) {
					reader.close();
				}
				if (inputStream!=null) {
					inputStream.close();
				}	
			}
		}
	}

	private String generateDocumentId(String source) {
//		TODO implement docId generation based on arxiv.txt filename or WOS:xxx id
		return source;
	}
	
	private String generateProjectId(String source) {
//		TODO implement project id generation based on 5 or 6 digit project number
		return source;
	}
	
	

}
