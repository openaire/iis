package eu.dnetlib.iis.wf.importer.content;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.DEFAULT_CSV_DELIMITER;
import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_APPROVED_DATASOURCES_CSV;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_APPROVED_OBJECSTORES_CSV;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_APPROVER_SIZELIMIT_MEGABYTES;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_CONNECTION_TIMEOUT;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_LOOKUP_SERVICE_LOC;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_OBJECSTORE_PAGESIZE;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_OBJECT_STORE_LOC;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_READ_TIMEOUT;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.enabling.resultset.client.ResultSetClientFactory;
import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.importer.schemas.DocumentContent;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.wf.importer.AvroWriterRunnable;
import eu.dnetlib.iis.wf.importer.ObjectWithPath;
import eu.dnetlib.iis.wf.importer.Poison;
import eu.dnetlib.iis.wf.importer.content.appover.ComplexContentApprover;
import eu.dnetlib.iis.wf.importer.content.appover.ContentApprover;
import eu.dnetlib.iis.wf.importer.content.appover.PDFHeaderBasedContentApprover;
import eu.dnetlib.iis.wf.importer.content.appover.SizeLimitContentApprover;

/**
 * {@link ObjectStoreService} based content importer.
 * Imports PDF contents to {@link DocumentContent} datastore and
 * plaintexts into {@link DocumentText} dataset.
 * @author mhorst
 * @deprecated use {@link ObjectStoreDocumentContentUrlImporterProcess} and 
 * {@link DocumentContentUrlBasedImporterMapper} pair of importers instead. 
 *
 */
@Deprecated
public class ObjectStoreContentImporter implements Process {

	private final static Logger log = Logger.getLogger(ObjectStoreContentImporter.class);
	
	private static final String PORT_OUT_DOCUMENT_TEXT = "document_text";
	
	private static final String PORT_OUT_DOCUMENT_CONTENT = "document_content";
	
	private static final Map<String, PortType> outputPorts = new HashMap<String, PortType>();
	
	private final int defaultPagesize = 100;
	
	/**
	 * Max queue size.
	 */
	private int queueSize = 5;
	
	/**
	 * Progress log interval.
	 */
	private int progresLogInterval = 100;
	
	/**
	 * Default PDF mime types.
	 */
	private Collection<String> mimeTypesPdf;
	
	
	/**
	 * Default text mime types.
	 */
	private Collection<String> mimeTypesText;
	
	{
		outputPorts.put(PORT_OUT_DOCUMENT_TEXT, 
				new AvroPortType(DocumentText.SCHEMA$));
		outputPorts.put(PORT_OUT_DOCUMENT_CONTENT, 
				new AvroPortType(DocumentContent.SCHEMA$));
	}
	
	/**
	 * Default constructor.
	 */
	public ObjectStoreContentImporter() {
		this.mimeTypesPdf = new HashSet<String>();
		this.mimeTypesPdf.add("application/pdf");
		this.mimeTypesPdf.add("pdf");
		this.mimeTypesText = new HashSet<String>();
		this.mimeTypesText.add("text/plain");
		this.mimeTypesText.add("text");
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
		int connectionTimeout = parameters.containsKey(
				IMPORT_CONTENT_CONNECTION_TIMEOUT)?
						Integer.valueOf(parameters.get(IMPORT_CONTENT_CONNECTION_TIMEOUT)):
							conf.getInt(
									IMPORT_CONTENT_CONNECTION_TIMEOUT, 60000);
		int readTimeout = parameters.containsKey(
				IMPORT_CONTENT_READ_TIMEOUT)?
						Integer.valueOf(parameters.get(IMPORT_CONTENT_READ_TIMEOUT)):
							conf.getInt(
									IMPORT_CONTENT_READ_TIMEOUT, 60000);

						ContentApprover contentApprover;
		int sizeLimitMegabytes = parameters.containsKey(
				IMPORT_CONTENT_APPROVER_SIZELIMIT_MEGABYTES)?
						Integer.valueOf(parameters.get(IMPORT_CONTENT_APPROVER_SIZELIMIT_MEGABYTES)):
							conf.getInt(
									IMPORT_CONTENT_APPROVER_SIZELIMIT_MEGABYTES,-1);
		if (sizeLimitMegabytes>0) {
			contentApprover = new ComplexContentApprover(
					new PDFHeaderBasedContentApprover(),
					new SizeLimitContentApprover(sizeLimitMegabytes));
		} else {
			contentApprover = new PDFHeaderBasedContentApprover();
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
		String[] objectStoreIds = null;
		String objectStoresCSV = parameters.containsKey(
				IMPORT_CONTENT_APPROVED_OBJECSTORES_CSV)?
						parameters.get(IMPORT_CONTENT_APPROVED_OBJECSTORES_CSV):
							conf.get(
									IMPORT_CONTENT_APPROVED_OBJECSTORES_CSV);
		if (objectStoresCSV!=null && !objectStoresCSV.isEmpty() && 
				!UNDEFINED_NONEMPTY_VALUE.equals(objectStoresCSV)) {
			objectStoreIds = StringUtils.split(objectStoresCSV, 
					DEFAULT_CSV_DELIMITER);
		} else {
//			looking for data sources
			String datasourcesCSV = parameters.containsKey(
					IMPORT_APPROVED_DATASOURCES_CSV)?
							parameters.get(IMPORT_APPROVED_DATASOURCES_CSV):
								conf.get(
										IMPORT_APPROVED_DATASOURCES_CSV);
			if (datasourcesCSV==null || datasourcesCSV.isEmpty() || 
					UNDEFINED_NONEMPTY_VALUE.equals(datasourcesCSV)) {
				log.warn("unable to locate object stores containing contents: neither '" + 
						IMPORT_CONTENT_APPROVED_OBJECSTORES_CSV + "' nor '" + 
						IMPORT_APPROVED_DATASOURCES_CSV + "' parameter provided! "
								+ "Empty content and text datastores will be created!");
				objectStoreIds = new String[0];
			} else {
//				finding objectstores based on datasources utilizing ISLookup service
				String lookupServiceLocation = parameters.containsKey(
						IMPORT_CONTENT_LOOKUP_SERVICE_LOC)?
								parameters.get(IMPORT_CONTENT_LOOKUP_SERVICE_LOC):
									conf.get(
											IMPORT_CONTENT_LOOKUP_SERVICE_LOC);
				if (lookupServiceLocation == null || lookupServiceLocation.isEmpty()) {
					throw new RuntimeException("unable to get objectstore id based on datasource id, "
							+ "unknown IS Lookup service location: no parameter provided: '" + 
							IMPORT_CONTENT_LOOKUP_SERVICE_LOC + "'");
				}
				W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
				eprBuilder = new W3CEndpointReferenceBuilder();
				eprBuilder.address(lookupServiceLocation);
				eprBuilder.build();
				ISLookUpService lookupService = new JaxwsServiceResolverImpl().getService(
						ISLookUpService.class, eprBuilder.build());
				String[] datasourceIds = StringUtils.split(datasourcesCSV, 
						DEFAULT_CSV_DELIMITER);
				objectStoreIds = new String[datasourceIds.length];
				for (int i=0; i<datasourceIds.length; i++) {
					objectStoreIds[i] = ObjectStoreContentProviderUtils.objectStoreIdLookup(
							lookupService, datasourceIds[i]);
				}
			}
		}

//		instantiating object store service
		W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
		eprBuilder.address(objectStoreLocation);
		eprBuilder.build();
		ObjectStoreService objectStore = new JaxwsServiceResolverImpl().getService(
				ObjectStoreService.class, eprBuilder.build());
		
		Path targetContentPath = portBindings.getOutput().get(PORT_OUT_DOCUMENT_CONTENT);
		Path targetPlaintextPath = portBindings.getOutput().get(PORT_OUT_DOCUMENT_TEXT);
		
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
		log.warn("starting content retrieval...");
		for (String currentObjectStoreId : objectStoreIds) {
			log.warn("starting importing process from object store: " + currentObjectStoreId);
			W3CEndpointReference objStoreResults = objectStore.deliverObjects(
					currentObjectStoreId, 
					0l,
	                System.currentTimeMillis());
			log.warn("obtained ObjectStore ResultSet EPR: " + objStoreResults.toString());

//			obtaining resultSet
			ResultSetClientFactory rsFactory = new ResultSetClientFactory();
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
					String resourceLoc = objStoreFile.getURI();
					
					if (mimeTypesPdf.contains(objStoreFile.getMimeType())) {
//						storing as content
						long startTimeContent = System.currentTimeMillis();
						byte[] content = ObjectStoreContentProviderUtils.getContentFromURL(
								resourceLoc, connectionTimeout, readTimeout);
						log.warn("content retrieval for id: " + objStoreFile.getObjectID() + 
								" and location: " + resourceLoc + " took: " +
								(System.currentTimeMillis()-startTimeContent) + " ms, got content: " +
								(content!=null && content.length>0));
						if (contentApprover.approve(content)) {
							DocumentContent.Builder documentContentBuilder = DocumentContent.newBuilder();
							documentContentBuilder.setId(resultId);
							if (content!=null) {
								documentContentBuilder.setPdf(ByteBuffer.wrap(
										content));
							}
							ObjectWithPath objWithPath = new ObjectWithPath(
									documentContentBuilder.build(), targetContentPath);
							boolean consumed = false;
							while (!consumed) {
								consumed = contentsQueue.offer(objWithPath, 10, TimeUnit.SECONDS);
								if (contentWriterRunnable.isWasInterrupted()) {
									throw new Exception("worker thread was interrupted, exitting");
								}
							}
							log.warn("content " + objStoreFile.getObjectID() + " retrieved successfully " +
									"for location: " + resourceLoc);
							} else {
								log.warn("content " + objStoreFile.getObjectID() + " not approved " +
										"for location: " + resourceLoc);
							}

					} else if (mimeTypesText.contains(objStoreFile.getMimeType())) {
//						storing as text
						long startTimeContent = System.currentTimeMillis();
						byte[] textContent = ObjectStoreContentProviderUtils.getContentFromURL(
								resourceLoc, connectionTimeout, readTimeout);
						log.warn("text content retrieval for id: " + objStoreFile.getObjectID() + 
								" and location: " + resourceLoc + " took: " +
								(System.currentTimeMillis()-startTimeContent)/1000 + " secs, got text content: " +
								(textContent!=null && textContent.length>0));
						DocumentText.Builder documentTextBuilder = DocumentText.newBuilder();
						documentTextBuilder.setId(resultId);
						if (textContent!=null) {
							documentTextBuilder.setText(new String(textContent, 
									ObjectStoreContentProviderUtils.defaultEncoding));
						}
						ObjectWithPath objWithPath = new ObjectWithPath(
								documentTextBuilder.build(), targetPlaintextPath);
						boolean consumed = false;
						while (!consumed) {
							consumed = contentsQueue.offer(objWithPath, 10, TimeUnit.SECONDS);
							if (contentWriterRunnable.isWasInterrupted()) {
								throw new Exception("worker thread was interrupted, exitting");
							}
						}
						log.warn("text content " + objStoreFile.getObjectID() + " retrieved successfully " +
								"for location: " + resourceLoc);
					} else {
						log.warn("got unhandled mime type: " + objStoreFile.getMimeType() + 
								" for object: " + objStoreFile.getObjectID());
					}
				}
				
				if (sourceIdx>0 && sourceIdx%progresLogInterval==0) {
					log.warn("content retrieval progress: " + sourceIdx + ", time taken to process " +
							progresLogInterval + " elements: " +
						((System.currentTimeMillis() - intervalTime)/60000) + " mins");
					intervalTime = System.currentTimeMillis();
				}
				sourceIdx++;
			}
			log.warn("importing process from object store: " + currentObjectStoreId + " has finished");
		}
		
		contentsQueue.add(new Poison());
		log.warn("waiting for writer thread finishing writing content");
		contentExecutorFuture.get();
		writerExecutor.shutdown();
		log.warn("content retrieval for "+sourceIdx+" documents finished in " + 
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
	
	/**
	 * Overwrites default PDF mime types.
	 * @param mimeTypesPdf
	 */
	public void setMimeTypesPdf(Collection<String> mimeTypesPdf) {
		this.mimeTypesPdf = mimeTypesPdf;
	}

	/**
	 * Overwrites default text mime types.
	 * @param mimeTypesText
	 */
	public void setMimeTypesText(Collection<String> mimeTypesText) {
		this.mimeTypesText = mimeTypesText;
	}

}
