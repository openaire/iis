package eu.dnetlib.iis.wf.importer.dataset;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.*;

import java.io.StringReader;
import java.security.InvalidParameterException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.ws.wsaddressing.W3CEndpointReference;
import javax.xml.ws.wsaddressing.W3CEndpointReferenceBuilder;

import org.apache.avro.file.DataFileWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;

import eu.dnetlib.data.mdstore.MDStoreService;
import eu.dnetlib.enabling.resultset.client.ResultSetClientFactory;
import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.importer.schemas.DataSetReference;
import eu.dnetlib.iis.importer.schemas.DocumentToMDStore;
import eu.dnetlib.iis.wf.importer.DataFileRecordReceiver;
/**
 * Process module importing dataset identifiers from datacite xml dump
 * and writing output to avro datastore.
 * @author mhorst
 *
 */
public class DataciteMDStoreImporter implements Process {
	
	private static final String PORT_OUT_DATASET = "dataset";
	private static final String PORT_OUT_DATASET_TO_MDSTORE = "dataset_to_mdstore";
	
	private final Logger log = Logger.getLogger(this.getClass());
	
	private final int defaultPagesize = 100;
	
	private final int progressLogInterval = 100000;
	
	private static final Map<String, PortType> outputPorts = new HashMap<String, PortType>();
	
	{
		outputPorts.put(PORT_OUT_DATASET, 
				new AvroPortType(DataSetReference.SCHEMA$));
		outputPorts.put(PORT_OUT_DATASET_TO_MDSTORE, 
				new AvroPortType(DocumentToMDStore.SCHEMA$));
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
		if (!parameters.containsKey(IMPORT_DATACITE_MDSTORE_SERVICE_LOCATION)) {
			throw new InvalidParameterException("unknown MDStore service location, "
					+ "required parameter '" + IMPORT_DATACITE_MDSTORE_SERVICE_LOCATION + "' is missing!");
		}
		if (!parameters.containsKey(IMPORT_DATACITE_MDSTORE_IDS_CSV)) {
			throw new InvalidParameterException("unknown MDStore identifier, "
					+ "required parameter '" + IMPORT_DATACITE_MDSTORE_IDS_CSV + "' is missing!");
		}
		
//		setting result set client read timeout
		Long rsClientReadTimeout = null;
		if (parameters.containsKey(IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT)) {
			rsClientReadTimeout = Long.valueOf(
					parameters.get(IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT));
		}
		
		DataFileWriter<DataSetReference> datasetRefWriter = null;
		DataFileWriter<DocumentToMDStore> datasetToMDStoreWriter = null;
		try {
//			initializing avro datastore writers
			datasetRefWriter = DataStore.create(
					new FileSystemPath(fs, portBindings.getOutput().get(PORT_OUT_DATASET)), 
					DataSetReference.SCHEMA$);
			datasetToMDStoreWriter = DataStore.create(
					new FileSystemPath(fs, portBindings.getOutput().get(PORT_OUT_DATASET_TO_MDSTORE)), 
					DocumentToMDStore.SCHEMA$);
			
//			initializing MDStore reader
			W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
			eprBuilder.address(parameters.get(IMPORT_DATACITE_MDSTORE_SERVICE_LOCATION));
			eprBuilder.build();
			MDStoreService mdStore = new JaxwsServiceResolverImpl().getService(
					MDStoreService.class, eprBuilder.build());
			String mdStoresCSV = parameters.get(IMPORT_DATACITE_MDSTORE_IDS_CSV);
			if (mdStoresCSV!=null && !mdStoresCSV.isEmpty() && 
					!WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(mdStoresCSV)) {
				String[] mdStoreIds = StringUtils.split(mdStoresCSV,
						WorkflowRuntimeParameters.DEFAULT_CSV_DELIMITER);
				for (String currentMdStoreId : mdStoreIds) {
					W3CEndpointReference eprResult = mdStore.deliverMDRecords(
							currentMdStoreId, null, null, null);
					log.warn("processing mdstore: " + currentMdStoreId + 
							" and obtained ResultSet EPR: " + eprResult.toString());
//					obtaining resultSet
					ResultSetClientFactory rsFactory = new ResultSetClientFactory();
					if (rsClientReadTimeout!=null) {
						rsFactory.setTimeout(rsClientReadTimeout);	
					}
					rsFactory.setServiceResolver(new JaxwsServiceResolverImpl());
					rsFactory.setPageSize(parameters.containsKey(IMPORT_DATACITE_MDSTORE_PAGESIZE)?
									Integer.valueOf(parameters.get(IMPORT_DATACITE_MDSTORE_PAGESIZE)):
										defaultPagesize);
					SAXParserFactory parserFactory = SAXParserFactory.newInstance();
					SAXParser saxParser = parserFactory.newSAXParser();
					int currentCount = 0;
					long startTime = System.currentTimeMillis();
					for (String record : rsFactory.getClient(eprResult)) {
						DataciteDumpXmlHandler handler = new DataciteDumpXmlHandler(
								new DataFileRecordReceiver<DataSetReference>(datasetRefWriter), 
								new DataFileRecordReceiver<DocumentToMDStore>(datasetToMDStoreWriter), 
								currentMdStoreId);
						saxParser.parse(new InputSource(new StringReader(record)), handler);
						currentCount++;
						if (currentCount%progressLogInterval==0) {
							log.warn("current progress: " + currentCount + 
									", last package of " + progressLogInterval + 
									" processed in " + ((System.currentTimeMillis()-startTime)/1000) + " secs");
							startTime = System.currentTimeMillis();
						}
					}
					log.warn("total number of processed records for mdstore " + 
							currentMdStoreId + ": "	+ currentCount);
				}
			} else {
				log.warn("got undefined mdstores list for datacite import, skipping!");
			}
		} finally {
			if (datasetRefWriter!=null) {
				datasetRefWriter.close();	
			}	
			if (datasetToMDStoreWriter!=null) {
				datasetToMDStoreWriter.close();	
			}
		}	
	}
	
}
