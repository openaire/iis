package eu.dnetlib.iis.wf.importer.dataset;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_DATACITE_MDSTORE_IDS_CSV;

import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.avro.file.DataFileWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.counter.NamedCounters;
import eu.dnetlib.iis.common.counter.NamedCountersFileWriter;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.importer.schemas.DataSetReference;
import eu.dnetlib.iis.importer.schemas.DocumentToMDStore;
import eu.dnetlib.iis.wf.importer.DataFileRecordReceiverWithCounter;
import eu.dnetlib.iis.wf.importer.facade.MDStoreFacade;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeUtils;
/**
 * Process module importing dataset identifiers from datacite xml dump and writing output to avro datastore.
 * 
 * @author mhorst
 *
 */
public class DataciteMDStoreImporter implements Process {
	
	private static final String PORT_OUT_DATASET = "dataset";
	private static final String PORT_OUT_DATASET_TO_MDSTORE = "dataset_to_mdstore";
	
	private static final String DATASET_COUNTER_NAME = "DATASET_COUNTER";
	private static final String DATASET_TO_MDSTORE_COUNTER_NAME = "DATASET_TO_MDSTORE_COUNTER";
	
	private final Logger log = Logger.getLogger(this.getClass());
	
	private final int progressLogInterval = 100000;
	
	private final NamedCountersFileWriter countersWriter = new NamedCountersFileWriter();
	
	private static final Map<String, PortType> outputPorts = new HashMap<String, PortType>();
	
	{
		outputPorts.put(PORT_OUT_DATASET, new AvroPortType(DataSetReference.SCHEMA$));
		outputPorts.put(PORT_OUT_DATASET_TO_MDSTORE, new AvroPortType(DocumentToMDStore.SCHEMA$));
	}
	
	//------------------------ LOGIC --------------------------
	
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
		
		Preconditions.checkArgument(parameters.containsKey(IMPORT_DATACITE_MDSTORE_IDS_CSV), 
                "unspecified MDStore identifiers, required parameter '%s' is missing!", IMPORT_DATACITE_MDSTORE_IDS_CSV);
		
		FileSystem fs = FileSystem.get(conf);
		
        try (DataFileWriter<DataSetReference> datasetRefWriter = DataStore.create(
                new FileSystemPath(fs, portBindings.getOutput().get(PORT_OUT_DATASET)), DataSetReference.SCHEMA$);
                DataFileWriter<DocumentToMDStore> datasetToMDStoreWriter = DataStore.create(
                        new FileSystemPath(fs, portBindings.getOutput().get(PORT_OUT_DATASET_TO_MDSTORE)),
                        DocumentToMDStore.SCHEMA$)) {

			NamedCounters counters = new NamedCounters(new String[] { DATASET_COUNTER_NAME, DATASET_TO_MDSTORE_COUNTER_NAME });
			
//			initializing MDStore reader
            MDStoreFacade mdStoreFacade = ServiceFacadeUtils.instantiate(parameters);
			
			String mdStoresCSV = parameters.get(IMPORT_DATACITE_MDSTORE_IDS_CSV);
			if (StringUtils.isNotBlank(mdStoresCSV) && 
					!WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(mdStoresCSV)) {
				String[] mdStoreIds = StringUtils.split(mdStoresCSV, WorkflowRuntimeParameters.DEFAULT_CSV_DELIMITER);
                
				SAXParserFactory parserFactory = SAXParserFactory.newInstance();
                SAXParser saxParser = parserFactory.newSAXParser();
				
                for (String currentMdStoreId : mdStoreIds) {
					int currentCount = 0;
					long startTime = System.currentTimeMillis();
					for (String record : mdStoreFacade.deliverMDRecords(currentMdStoreId)) {
						DataFileRecordReceiverWithCounter<DataSetReference> datasetReceiver = new DataFileRecordReceiverWithCounter<>(datasetRefWriter);
						DataFileRecordReceiverWithCounter<DocumentToMDStore> datasetToMDStoreReceiver = new DataFileRecordReceiverWithCounter<>(datasetToMDStoreWriter);
						
						DataciteDumpXmlHandler handler = new DataciteDumpXmlHandler(datasetReceiver, datasetToMDStoreReceiver, currentMdStoreId);
						saxParser.parse(new InputSource(new StringReader(record)), handler);
						
						counters.increment(DATASET_COUNTER_NAME, datasetReceiver.getReceivedCount());
						counters.increment(DATASET_TO_MDSTORE_COUNTER_NAME, datasetToMDStoreReceiver.getReceivedCount());
						
						currentCount++;
						if (currentCount%progressLogInterval==0) {
							log.debug("current progress: " + currentCount + ", last package of " + progressLogInterval + 
									" processed in " + ((System.currentTimeMillis()-startTime)/1000) + " secs");
							startTime = System.currentTimeMillis();
						}
					}
					log.debug("total number of processed records for mdstore " + currentMdStoreId + ": "	+ currentCount);
				}
			} else {
				log.warn("got undefined mdstores list for datacite import, skipping!");
			}
			
			countersWriter.writeCounters(counters, System.getProperty("oozie.action.output.properties"));
		}
	}
	
}
