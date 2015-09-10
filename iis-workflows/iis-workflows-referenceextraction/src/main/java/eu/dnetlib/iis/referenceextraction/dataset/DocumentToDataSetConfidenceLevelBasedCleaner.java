package eu.dnetlib.iis.referenceextraction.dataset;

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
import eu.dnetlib.iis.referenceextraction.dataset.schemas.DocumentToDataSet;

/**
 * Confidence level based {@link DocumentToDataSet} relations cleaner.
 * @author mhorst
 *
 */
public class DocumentToDataSetConfidenceLevelBasedCleaner implements eu.dnetlib.iis.core.java.Process {

	public static final String CONFIDENCE_LEVEL_THRESHOLD = "export.document_to_dataset.confidence.level.threshold";
	
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
				new AvroPortType(DocumentToDataSet.SCHEMA$));
		return inputPorts;
	}
	
	private static HashMap<String, PortType> createOutputPorts(){
		HashMap<String, PortType> outputPorts = 
				new HashMap<String, PortType>();
		outputPorts.put(outputPort, 
				new AvroPortType(DocumentToDataSet.SCHEMA$));
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
		
		CloseableIterator<DocumentToDataSet> it = DataStore.getReader(
				new FileSystemPath(fs, input.get(inputPort)));
		DataFileWriter<DocumentToDataSet> writer = DataStore.create(
				new FileSystemPath(fs, output.get(outputPort)), 
				DocumentToDataSet.SCHEMA$);
		try {
			while (it.hasNext()) {
				DocumentToDataSet current = it.next();
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

