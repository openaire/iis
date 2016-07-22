package eu.dnetlib.iis.wf.importer.content;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.DEFAULT_CSV_DELIMITER;
import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_APPROVED_OBJECSTORES_CSV;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_BLACKLISTED_OBJECSTORES_CSV;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_OBJECSTORE_PAGESIZE;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_OBJECT_STORE_LOC;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.xml.ws.wsaddressing.W3CEndpointReference;
import javax.xml.ws.wsaddressing.W3CEndpointReferenceBuilder;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import eu.dnetlib.data.objectstore.rmi.ObjectStoreFile;
import eu.dnetlib.data.objectstore.rmi.ObjectStoreService;
import eu.dnetlib.enabling.resultset.client.ResultSetClientFactory;
import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;
import eu.dnetlib.iis.wf.importer.AvroWriterRunnable;
import eu.dnetlib.iis.wf.importer.ObjectWithPath;
import eu.dnetlib.iis.wf.importer.Poison;

/**
 * {@link ObjectStoreService} based content importer.
 * Imports identifiers and data location pairs into {@link DocumentContentUrl} datastore.
 * @author mhorst
 *
 */
public class ObjectStoreDocumentContentUrlImporterProcess implements Process {

	private final static Logger log = Logger.getLogger(ObjectStoreDocumentContentUrlImporterProcess.class);
	
	private static final String PORT_OUT_DOCUMENT_CONTENT_URL = "content_url";
	
	private static final Map<String, PortType> outputPorts = new HashMap<String, PortType>();
	
	private final int defaultPagesize = 100;
	
	/**
	 * Max queue size.
	 */
	private int queueSize = 5;
	
	/**
	 * Progress log interval.
	 */
	private int progresLogInterval = 100000;
	
	{
		outputPorts.put(PORT_OUT_DOCUMENT_CONTENT_URL, 
				new AvroPortType(DocumentContentUrl.SCHEMA$));
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
//		setting result set client read timeout
		Long rsClientReadTimeout = null;
		if (parameters.containsKey(IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT)) {
			rsClientReadTimeout = Long.valueOf(
					parameters.get(IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT));
		}
		
//		looking for object stores
		String objectStoreLocation = parameters.containsKey(
				IMPORT_CONTENT_OBJECT_STORE_LOC)?
						parameters.get(IMPORT_CONTENT_OBJECT_STORE_LOC):
							conf.get(
									IMPORT_CONTENT_OBJECT_STORE_LOC);
		if (objectStoreLocation == null || objectStoreLocation.isEmpty()) {
			throw new RuntimeException("unknown object store service location: "
					+ "no parameter provided: '" + 
					IMPORT_CONTENT_OBJECT_STORE_LOC + "'");
		}
		
//		blacklisting objectstores
		Set<String> blacklistedObjectStoreIds = null;
		String blacklistedObjectStoresCSV = parameters.containsKey(
				IMPORT_CONTENT_BLACKLISTED_OBJECSTORES_CSV)?
						parameters.get(IMPORT_CONTENT_BLACKLISTED_OBJECSTORES_CSV):
							conf.get(
									IMPORT_CONTENT_BLACKLISTED_OBJECSTORES_CSV);
		if (blacklistedObjectStoresCSV!=null && !blacklistedObjectStoresCSV.isEmpty() && 
				!UNDEFINED_NONEMPTY_VALUE.equals(blacklistedObjectStoresCSV)) {
			blacklistedObjectStoreIds = new HashSet<String>(Arrays.asList(StringUtils.split(blacklistedObjectStoresCSV, 
					DEFAULT_CSV_DELIMITER)));
		} else {
			blacklistedObjectStoreIds = Collections.emptySet();
		}

//		
		String[] objectStoreIds = null;
		String objectStoresCSV = parameters.containsKey(
				IMPORT_CONTENT_APPROVED_OBJECSTORES_CSV)?
						parameters.get(IMPORT_CONTENT_APPROVED_OBJECSTORES_CSV):
							conf.get(
									IMPORT_CONTENT_APPROVED_OBJECSTORES_CSV);
		if (StringUtils.isNotBlank(objectStoresCSV) && 
				!UNDEFINED_NONEMPTY_VALUE.equals(objectStoresCSV)) {
			objectStoreIds = StringUtils.split(objectStoresCSV, 
					DEFAULT_CSV_DELIMITER);
		} else {
		    log.warn("no object stores provided, empty results will be returned");
		    objectStoreIds = new String[0];
		}
//		instantiating object store service
		W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
		eprBuilder.address(objectStoreLocation);
		eprBuilder.build();
		ObjectStoreService objectStore = new JaxwsServiceResolverImpl().getService(
				ObjectStoreService.class, eprBuilder.build());
		
		final Path targetContentUrlPath = portBindings.getOutput().get(PORT_OUT_DOCUMENT_CONTENT_URL);
		
		FileSystem fs = FileSystem.get(conf);
		BlockingQueue<ObjectWithPath> contentsQueue = new ArrayBlockingQueue<ObjectWithPath>(
				queueSize);
		AvroWriterRunnable contentWriterRunnable = new AvroWriterRunnable(
				fs, contentsQueue, 1);
		
		ExecutorService writerExecutor = Executors.newSingleThreadExecutor(); 
		Future<?> contentExecutorFuture = writerExecutor.submit(contentWriterRunnable);
		
		long startTime = System.currentTimeMillis();
		long intervalTime = startTime;
		
		int sourceIdx=0;
		log.debug("starting url retrieval...");
		for (String currentObjectStoreId : objectStoreIds) {
			if (blacklistedObjectStoreIds.contains(currentObjectStoreId)) {
				log.debug("skipping blacklisted objectstore: " + currentObjectStoreId);
				continue;
			}
			
			log.debug("starting importing process from object store: " + currentObjectStoreId);
			W3CEndpointReference objStoreResults = objectStore.deliverObjects(
					currentObjectStoreId, 
					0l,
	                System.currentTimeMillis());
			log.debug("obtained ObjectStore ResultSet EPR: " + objStoreResults.toString());

//			obtaining resultSet
			ResultSetClientFactory rsFactory = new ResultSetClientFactory();
			if (rsClientReadTimeout!=null) {
				rsFactory.setTimeout(rsClientReadTimeout);	
			}
			rsFactory.setServiceResolver(new JaxwsServiceResolverImpl());
			rsFactory.setPageSize(parameters.containsKey(
					IMPORT_CONTENT_OBJECSTORE_PAGESIZE)?
							Integer.valueOf(parameters.get(
									IMPORT_CONTENT_OBJECSTORE_PAGESIZE)):
								defaultPagesize);
			for (String record : rsFactory.getClient(objStoreResults)) {
				ObjectStoreFile objStoreFile = ObjectStoreFile.createObject(record);
				if (objStoreFile!=null) {
					String resultId = ObjectStoreContentProviderUtils.extractResultIdFromObjectId(
							objStoreFile.getObjectID());
					DocumentContentUrl.Builder documentContentUrlBuilder = DocumentContentUrl.newBuilder();
					documentContentUrlBuilder.setId(resultId);
					documentContentUrlBuilder.setUrl(objStoreFile.getURI());
					documentContentUrlBuilder.setMimeType(objStoreFile.getMimeType());
					documentContentUrlBuilder.setContentChecksum(objStoreFile.getMd5Sum());
					documentContentUrlBuilder.setContentSizeKB(objStoreFile.getFileSizeKB());
					
					ObjectWithPath objWithPath = new ObjectWithPath(
							documentContentUrlBuilder.build(), targetContentUrlPath);
					boolean consumed = false;
					while (!consumed) {
						consumed = contentsQueue.offer(objWithPath, 10, TimeUnit.SECONDS);
						if (contentWriterRunnable.isWasInterrupted()) {
							throw new Exception("worker thread was interrupted, exitting");
						}
					}	
				}
				
				if (sourceIdx>0 && sourceIdx%progresLogInterval==0) {
					log.debug("content retrieval progress: " + sourceIdx + ", time taken to process " +
							progresLogInterval + " elements: " +
						((System.currentTimeMillis() - intervalTime)/1000) + " secs");
					intervalTime = System.currentTimeMillis();
				}
				sourceIdx++;
			}
			log.debug("URL importing process from object store: " + currentObjectStoreId + " has finished");
		}
		
		contentsQueue.add(new Poison());
		log.debug("waiting for writer thread finishing writing content");
		contentExecutorFuture.get();
		writerExecutor.shutdown();
		log.debug("content retrieval for "+sourceIdx+" documents finished in " + 
				((System.currentTimeMillis()-startTime)/1000) + " secs");
	}
	
	/**
	 * Sets queue size.
	 * @param queueSize
	 */
	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}
	
	/**
	 * Sets progress log interval.
	 * @param progresLogInterval
	 */
	public void setProgresLogInterval(int progresLogInterval) {
		this.progresLogInterval = progresLogInterval;
	}

}
