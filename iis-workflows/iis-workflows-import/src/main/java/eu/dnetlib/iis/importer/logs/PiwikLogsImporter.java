package eu.dnetlib.iis.importer.logs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.security.InvalidParameterException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import eu.dnetlib.iis.core.java.PortBindings;
import eu.dnetlib.iis.core.java.Process;
import eu.dnetlib.iis.core.java.io.DataStore;
import eu.dnetlib.iis.core.java.io.FileSystemPath;
import eu.dnetlib.iis.core.java.porttype.AvroPortType;
import eu.dnetlib.iis.core.java.porttype.PortType;
import eu.dnetlib.iis.websiteusage.schemas.LogEntry;

/**
 * Process module importing Piwik log entries and writing output to avro datastore.
 * @author mhorst
 *
 */
public class PiwikLogsImporter implements Process {
	
	private static final String NULL_VALUE = "NULL";
	
	private static final String TIMESTAMP_HEADER = "timestamp";
	
	private static final String PORT_OUT_LOG_ENTRIES = "log_entries";
	
	public static final String PARAM_PIWIK_LOGS_PATH = "import.piwik.logs.path";

	private final Logger log = Logger.getLogger(this.getClass());
	
	private static final Map<String, PortType> outputPorts = new HashMap<String, PortType>();
	
	{
		outputPorts.put(PORT_OUT_LOG_ENTRIES, 
				new AvroPortType(LogEntry.SCHEMA$));
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
		if (parameters.containsKey(PARAM_PIWIK_LOGS_PATH)) {
			DataFileWriter<LogEntry> logEntryWriter = null;
			try {
				logEntryWriter = DataStore.create(
						new FileSystemPath(fs, portBindings.getOutput().get(PORT_OUT_LOG_ENTRIES)), 
						LogEntry.SCHEMA$);
				processNode(fs, 
						new Path(parameters.get(PARAM_PIWIK_LOGS_PATH)),
						logEntryWriter);
			} finally {
				if (logEntryWriter!=null) {
					logEntryWriter.close();	
				}	
			}		
		} else {
			throw new InvalidParameterException("required parameter '" + 
					PARAM_PIWIK_LOGS_PATH + "' is missing!");
		}
	}

	protected void processNode(FileSystem fs, Path currentPath,
			DataFileWriter<LogEntry> datasetRefWriter) throws Exception {
		if (fs.isDirectory(currentPath)) {
			for (FileStatus fileStatus : fs.listStatus(currentPath)) {
				processNode(fs, fileStatus.getPath(), 
						datasetRefWriter);
			}
		} else {
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new InputStreamReader(fs.open(currentPath)));
				String line;
				while ((line=reader.readLine())!=null) {
					String[] splitted = line.split("\t", -1);
					if (splitted.length!=5) {
						log.warn("skipping invalid line:\n" + line + "\ncontaining " +  
								splitted.length +" tokens in file: " + currentPath.toString());
						continue;
					}
					if (TIMESTAMP_HEADER.equals(splitted[0])) {
						log.debug("skipping file header in path: " + currentPath.toString());
						continue;
					}
					LogEntry.Builder logBuilder = LogEntry.newBuilder();
					logBuilder.setTimestamp(splitted[0]);
					logBuilder.setAction(splitted[1]);
					if (!isEmpty(splitted[2])) {
						logBuilder.setUser(splitted[2]);	
					}
					if (!isEmpty(splitted[3])) {
						logBuilder.setSession(splitted[3]);
					}
					if (!isEmpty(splitted[4])) {
						logBuilder.setData(splitted[4]);
					}
					datasetRefWriter.append(logBuilder.build());
				}
			} finally {
				if (reader!=null) {
					reader.close();
				}	
			}
		}
	}
	
	private static boolean isEmpty(String field) {
		return field==null || field.trim().isEmpty() || NULL_VALUE.equals(field);
	}
}
