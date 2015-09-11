package eu.dnetlib.iis.workflows.referenceextraction.project;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import eu.dnetlib.iis.core.java.PortBindings;
import eu.dnetlib.iis.core.java.ProcessUtils;
import eu.dnetlib.iis.core.java.io.CloseableIterator;
import eu.dnetlib.iis.core.java.io.DataStore;
import eu.dnetlib.iis.core.java.io.FileSystemPath;
import eu.dnetlib.iis.core.java.porttype.AvroPortType;
import eu.dnetlib.iis.core.java.porttype.PortType;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;

/**
 * Confidence level based {@link DocumentToProject} relations cleaner.
 * @author mhorst
 *
 */
public class DocumentToProjectConfidenceLevelBasedCleaner implements eu.dnetlib.iis.core.java.Process {

	public static final String CONFIDENCE_LEVEL_THRESHOLD = "export.document_to_project.confidence.level.threshold";
	
	private final Logger log = Logger.getLogger(this.getClass());
	
	private final static String inputPort = "input";
	private final static String outputPort = "output";
	
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
		inputPorts.put(inputPort, 
				new AvroPortType(DocumentToProject.SCHEMA$));
		return inputPorts;
	}
	
	private static HashMap<String, PortType> createOutputPorts(){
		HashMap<String, PortType> outputPorts = 
				new HashMap<String, PortType>();
		outputPorts.put(outputPort, 
				new AvroPortType(DocumentToProject.SCHEMA$));
		return outputPorts;	
	}

	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters) throws Exception {
		String confidenceLevelThresholdStr = ProcessUtils.getParameterValue(
				CONFIDENCE_LEVEL_THRESHOLD, 
				conf, parameters);
		if (confidenceLevelThresholdStr==null || confidenceLevelThresholdStr.isEmpty()) {
			throw new RuntimeException("no confidence level threshold "
					+ "parameter provided: '" + CONFIDENCE_LEVEL_THRESHOLD + "'");
		}
		float confidenceLevelThreshold = Float.parseFloat(confidenceLevelThresholdStr);
		
		Map<String, Path> input = portBindings.getInput();
		Map<String, Path> output = portBindings.getOutput();
		
		FileSystem fs = FileSystem.get(conf);
		
		CloseableIterator<DocumentToProject> it = DataStore.getReader(
				new FileSystemPath(fs, input.get(inputPort)));
		DataFileWriter<DocumentToProject> writer = DataStore.create(
				new FileSystemPath(fs, output.get(outputPort)), 
				DocumentToProject.SCHEMA$);
		try {
			while (it.hasNext()) {
				DocumentToProject current = it.next();
				if (current.getConfidenceLevel()==null ||
						current.getConfidenceLevel()>=confidenceLevelThreshold) {
					writer.append(current);
				} else {
					log.warn("skipping relation, "
							+ "confidence level below the threshold "
							+ "("+ confidenceLevelThresholdStr	+"): " + 
							current.toString());
				}
			}	
		} finally {
			it.close();
			writer.close();
		}
	}
}
