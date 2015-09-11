package eu.dnetlib.iis.workflows.referenceextraction.dataset.importer;

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

import eu.dnetlib.iis.core.java.PortBindings;
import eu.dnetlib.iis.core.java.Process;
import eu.dnetlib.iis.core.java.ProcessUtils;
import eu.dnetlib.iis.core.java.io.DataStore;
import eu.dnetlib.iis.core.java.io.FileSystemPath;
import eu.dnetlib.iis.core.java.porttype.AvroPortType;
import eu.dnetlib.iis.core.java.porttype.PortType;
import eu.dnetlib.iis.referenceextraction.dataset.schemas.DocumentToDataSet;
import eu.dnetlib.iis.workflows.referenceextraction.shared.importer.SharedImporterUtils;

/**
 * Module importing data from existing CSV lists and producing
 * {@link DocumentToDataSet} avro datastore.
 * @author mhorst
 *
 */
public class DocumentToDataSetImporter implements Process {
	
	private final Logger log = Logger.getLogger(this.getClass());
	
	public static final String PARAM_CSV_PATH = "import.dataset.csv.path";

	private static final String outputPortName = "output";
	
	private static final String skipLinePrefix = "arxiv_id";
	
	private static final char separatorChar = ',';
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return Collections.emptyMap();
	}

	@Override
	public Map<String, PortType> getOutputPorts() {
		Map<String, PortType> output = new HashMap<String, PortType>();
		output.put(outputPortName, 
				new AvroPortType(DocumentToDataSet.SCHEMA$));
		return output;
	}

	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		String csvPath = ProcessUtils.getParameterValue(
				PARAM_CSV_PATH, conf, parameters);
		if (csvPath!=null && !csvPath.isEmpty()) {
			DataFileWriter<DocumentToDataSet> writer = null;
			try {
				writer = DataStore.create(
						new FileSystemPath(fs, portBindings.getOutput().get(outputPortName)), 
						DocumentToDataSet.SCHEMA$);
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
			DataFileWriter<DocumentToDataSet> writer) throws Exception {
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
					if (SharedImporterUtils.skipLine(line, skipLinePrefix)) {
						log.warn("skipping line: " + line);
						continue;
					} else {
						String[] split = StringUtils.split(
								line, separatorChar);
						if (split.length>=2) {
							DocumentToDataSet.Builder builder = DocumentToDataSet.newBuilder();
							builder.setDocumentId(generateDocumentId(split[0].trim()));
							builder.setDatasetId(generateDatasetId(split[1].trim()));
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
//		TODO implement docId generation based on arxiv.txt filename
		return source;
	}
	
	private String generateDatasetId(String source) {
//		TODO implement datasetId generation based on datacite id
		return source;
	}

}
