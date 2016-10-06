package eu.dnetlib.iis.wf.importer.content;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.DEFAULT_CSV_DELIMITER;
import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_APPROVED_OBJECSTORES_CSV;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_BLACKLISTED_OBJECSTORES_CSV;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import eu.dnetlib.data.objectstore.rmi.ObjectStoreFile;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;
import eu.dnetlib.iis.wf.importer.AvroWriterRunnable;
import eu.dnetlib.iis.wf.importer.ObjectWithPath;
import eu.dnetlib.iis.wf.importer.Poison;
import eu.dnetlib.iis.wf.importer.facade.ObjectStoreFacade;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeUtils;

/**
 * {@link ObjectStoreFacade} based content importer. 
 * 
 * Imports identifiers and data location pairs into {@link DocumentContentUrl} datastore.
 * 
 * @author mhorst
 *
 */
public class ObjectStoreDocumentContentUrlImporterProcess implements Process {

	private final static Logger log = Logger.getLogger(ObjectStoreDocumentContentUrlImporterProcess.class);
	
	private static final String PORT_OUT_DOCUMENT_CONTENT_URL = "content_url";
	
	private static final Map<String, PortType> outputPorts = new HashMap<String, PortType>();
	
	/**
	 * Max queue size.
	 */
	private int queueSize = 5;
	
	/**
	 * Progress log interval.
	 */
	private int progresLogInterval = 100000;
	
	{
		outputPorts.put(PORT_OUT_DOCUMENT_CONTENT_URL, new AvroPortType(DocumentContentUrl.SCHEMA$));
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
		
//		blacklisting objectstores
		Set<String> blacklistedObjectStoreIds = null;
		String blacklistedObjectStoresCSV = parameters.get(IMPORT_CONTENT_BLACKLISTED_OBJECSTORES_CSV);
		if (StringUtils.isNotBlank(blacklistedObjectStoresCSV) && !UNDEFINED_NONEMPTY_VALUE.equals(blacklistedObjectStoresCSV)) {
			blacklistedObjectStoreIds = Sets.newHashSet(StringUtils.split(blacklistedObjectStoresCSV, DEFAULT_CSV_DELIMITER)); 
		} else {
			blacklistedObjectStoreIds = Collections.emptySet();
		}
		
//		gathering set of objectstores
		String[] objectStoreIds = null;
		String objectStoresCSV = parameters.get(IMPORT_CONTENT_APPROVED_OBJECSTORES_CSV);
		if (StringUtils.isNotBlank(objectStoresCSV) && !UNDEFINED_NONEMPTY_VALUE.equals(objectStoresCSV)) {
			objectStoreIds = StringUtils.split(objectStoresCSV, DEFAULT_CSV_DELIMITER);
		} else {
		    log.warn("no object stores provided, empty results will be returned");
		    objectStoreIds = new String[0];
		}
		
//		instantiating object store service
		ObjectStoreFacade objectStoreFacade = ServiceFacadeUtils.instantiate(parameters);
		
		final Path targetContentUrlPath = portBindings.getOutput().get(PORT_OUT_DOCUMENT_CONTENT_URL);
		
		FileSystem fs = FileSystem.get(conf);
		BlockingQueue<ObjectWithPath> contentsQueue = new ArrayBlockingQueue<ObjectWithPath>(queueSize);
		AvroWriterRunnable contentWriterRunnable = new AvroWriterRunnable(fs, contentsQueue, 1);
		
		ExecutorService writerExecutor = Executors.newSingleThreadExecutor(); 
		Future<?> contentExecutorFuture = writerExecutor.submit(contentWriterRunnable);
		
		long startTime = System.currentTimeMillis();
		long intervalTime = startTime;
		
		int sourceIdx=0;
		log.info("starting url retrieval...");
		for (String currentObjectStoreId : objectStoreIds) {
			if (blacklistedObjectStoreIds.contains(currentObjectStoreId)) {
				log.info("skipping blacklisted objectstore: " + currentObjectStoreId);
				continue;
			}
			log.info("starting importing process from object store: " + currentObjectStoreId);
			for (String record : objectStoreFacade.deliverObjects(currentObjectStoreId, 0l, System.currentTimeMillis())) {
			    
			    processRecord(record, targetContentUrlPath, contentsQueue, contentWriterRunnable);
				
				if (sourceIdx>0 && sourceIdx%progresLogInterval==0) {
					log.info("content retrieval progress: " + sourceIdx + ", time taken to process " +
							progresLogInterval + " elements: " + ((System.currentTimeMillis() - intervalTime)/1000) + " secs");
					intervalTime = System.currentTimeMillis();
				}
				sourceIdx++;
			}
			log.info("URL importing process from object store: " + currentObjectStoreId + " has finished");
		}
		
		contentsQueue.add(new Poison());
		log.info("waiting for writer thread finishing writing content");
		contentExecutorFuture.get();
		writerExecutor.shutdown();
		log.info("content retrieval for "+sourceIdx+" documents finished in " + 
				((System.currentTimeMillis()-startTime)/1000) + " secs");
	}
	
    //------------------------ PRIVATE --------------------------
	
	/**
	 * Processes single metadata record encoded as JSON.
	 * 
	 * @param metaJsonRecord metadata record obtained from ObjectStore
	 * @param targetContentUrlPath target location
	 * @param contentsQueue queue reference
	 * @param contentWriterRunnable writer thread to be checked for interruption
	 */
	private void processRecord(String metaJsonRecord, Path targetContentUrlPath, 
	        BlockingQueue<ObjectWithPath> contentsQueue, AvroWriterRunnable contentWriterRunnable) throws Exception {
	    ObjectStoreFile objStoreFile = ObjectStoreFile.createObject(metaJsonRecord);
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
	}
	
    //------------------------ SETTERS --------------------------
	
	/**
	 * @param queueSize worker thread queue size
	 */
	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}
	
	/**
	 * @param progresLogInterval interval, expressed in number of records, for progress logging
	 */
	public void setProgresLogInterval(int progresLogInterval) {
		this.progresLogInterval = progresLogInterval;
	}

}
