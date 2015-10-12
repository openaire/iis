package eu.dnetlib.iis.workflows.export.actionmanager.entity.document;

import static eu.dnetlib.iis.workflows.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ACTION_SETID;
import static eu.dnetlib.iis.workflows.export.actionmanager.ExportWorkflowRuntimeParameters.IMPORT_DOCUMENT_MDSTORE_ID;
import static eu.dnetlib.iis.workflows.export.actionmanager.ExportWorkflowRuntimeParameters.IMPORT_DOCUMENT_MDSTORE_SERVICE_LOCATION;

import java.io.UnsupportedEncodingException;
import java.security.InvalidParameterException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.ws.wsaddressing.W3CEndpointReferenceBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import eu.dnetlib.actionmanager.actions.ActionFactory;
import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.actions.XsltInfoPackageAction;
import eu.dnetlib.actionmanager.common.Operation;
import eu.dnetlib.data.mdstore.DocumentNotFoundException;
import eu.dnetlib.data.mdstore.MDStoreService;
import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.core.java.PortBindings;
import eu.dnetlib.iis.core.java.Process;
import eu.dnetlib.iis.core.java.ProcessUtils;
import eu.dnetlib.iis.core.java.io.CloseableIterator;
import eu.dnetlib.iis.core.java.io.DataStore;
import eu.dnetlib.iis.core.java.io.FileSystemPath;
import eu.dnetlib.iis.core.java.porttype.AvroPortType;
import eu.dnetlib.iis.core.java.porttype.PortType;
import eu.dnetlib.iis.export.auxiliary.schemas.Identifier;
import eu.dnetlib.iis.workflows.export.actionmanager.api.ActionManagerServiceFacade;
import eu.dnetlib.iis.workflows.export.actionmanager.api.HBaseActionManagerServiceFacade;
import eu.dnetlib.iis.workflows.export.actionmanager.cfg.ActionManagerConfigurationProvider;
import eu.dnetlib.iis.workflows.export.actionmanager.cfg.StaticConfigurationProvider;

/**
 * Document entity exporter.
 * @author mhorst
 *
 */
public class DocumentExporterProcess implements Process {
	
	private final Logger log = Logger.getLogger(this.getClass());

	private final static String inputPort = "input";
	
	private final static String documentIdPrefix;
	
	static {
		try {
			documentIdPrefix = new String(HBaseConstants.ROW_PREFIX_RESULT,
					HBaseConstants.STATIC_FIELDS_ENCODING_UTF8);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return createInputPorts();
	}

	private static HashMap<String, PortType> createInputPorts(){
		HashMap<String, PortType> inputPorts = 
				new HashMap<String, PortType>();
		inputPorts.put(inputPort, 
				new AvroPortType(Identifier.SCHEMA$));
		return inputPorts;
	}
	
	@Override
	public Map<String, PortType> getOutputPorts() {
		return Collections.emptyMap();
	}

	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		String mdStoreLocation = ProcessUtils.getParameterValue(
				IMPORT_DOCUMENT_MDSTORE_SERVICE_LOCATION, 
				conf, parameters);
		String mdStoreId = ProcessUtils.getParameterValue(
				IMPORT_DOCUMENT_MDSTORE_ID, 
				conf, parameters);
		String actionSetId = ProcessUtils.getParameterValue(
				EXPORT_ACTION_SETID, 
				conf, parameters);
		
		if (mdStoreLocation==null || WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(mdStoreLocation)) {
			throw new InvalidParameterException("unable to export document entities to action manager, " + 
					"unknown MDStore service location. "
					+ "Required parameter '" + IMPORT_DOCUMENT_MDSTORE_SERVICE_LOCATION + "' is missing!");
		}
		if (mdStoreId==null || WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(mdStoreId)) {
			throw new InvalidParameterException("unable to export document entities to action manager, " +
					"unknown MDStore identifier. "
					+ "Required parameter '" + IMPORT_DOCUMENT_MDSTORE_ID + "' is missing!");
		}
		if (actionSetId==null || WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(actionSetId)) {
			throw new RuntimeException("unable to export document entities to action manager, " +
					"no '" + EXPORT_ACTION_SETID + "' required parameter provided!");
		}
		
		W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
		eprBuilder.address(mdStoreLocation);
		eprBuilder.build();
		MDStoreService mdStore = new JaxwsServiceResolverImpl().getService(
				MDStoreService.class, eprBuilder.build());
		
		CloseableIterator<Identifier> idsIt = DataStore.getReader(
				new FileSystemPath(fs, portBindings.getInput().get(inputPort)));
		
		Map<String,Resource> xslts = new HashMap<String, Resource>();
		String documentXSLT = "dmf2actions";
		xslts.put(documentXSLT, new ClassPathResource(
				"eu/dnetlib/actionmanager/xslt/dmf2insertActions.xslt"));
		ActionFactory actionFactory = new ActionFactory();
		actionFactory.setXslts(xslts);
		
		ActionManagerServiceFacade actionManager = new HBaseActionManagerServiceFacade(
				conf, parameters);
		ActionManagerConfigurationProvider configProvider = new StaticConfigurationProvider(
				StaticConfigurationProvider.AGENT_DEFAULT,
				StaticConfigurationProvider.PROVENANCE_DEFAULT,
				StaticConfigurationProvider.ACTION_TRUST_0_9,
				StaticConfigurationProvider.NAMESPACE_PREFIX_WOS);
		Set<String> exportedDocumentIds = new HashSet<String>();

		try {
			long timeSplit = System.currentTimeMillis();
			int counter = 0;
			while (idsIt.hasNext()) {
				String documentId = idsIt.next().getId().toString();
				if (!exportedDocumentIds.contains(documentId)) {
					try {
						String mdStoreRecord = mdStore.deliverRecord(
								mdStoreId, convertToMDStoreId(documentId));
						if (mdStoreRecord!=null) {
							XsltInfoPackageAction xsltAction = actionFactory.generateInfoPackageAction(
									documentXSLT, actionSetId, 
									configProvider.provideAgent(), 
									Operation.INSERT, mdStoreRecord,
									configProvider.provideProvenance(),
									configProvider.provideNamespacePrefix(),
									configProvider.provideActionTrust());
							if (xsltAction!=null) {
								List<AtomicAction> atomicActions = xsltAction.asAtomicActions();
								if (atomicActions!=null) {
									actionManager.storeAction(atomicActions,
											configProvider.provideProvenance(),
											configProvider.provideActionTrust(),
											configProvider.provideNamespacePrefix());	
								}
							}
							counter++;
							if (counter%10000==0) {
								log.warn("exported " + counter + " documents in " +
										((System.currentTimeMillis()-timeSplit)/1000) + " secs");
								timeSplit = System.currentTimeMillis();
							}
						}
						exportedDocumentIds.add(documentId);	
					} catch (DocumentNotFoundException e) {
						log.error("mdrecord: " + convertToMDStoreId(documentId) + 
									" wasn't found in mdstore: " + mdStoreId ,e);
//						TODO write missing document identifiers in output datastore
					}  catch (Exception e) {
						log.error("got exception when trying to retrieve "
								+ "MDStore record for mdstore id " + mdStoreId + 
								", and document id: " + convertToMDStoreId(documentId), e);
						throw e;
					}
				}
			}
			log.warn("exported " + counter + " documents in total");
		} finally {
			idsIt.close();
			actionManager.close();
		}
	}
	
	/**
	 * Converts to MDStore id by skipping result entity prefix.
	 * @param id
	 * @return MDStore compliant identifier
	 */
	private static final String convertToMDStoreId(String id) {
		if (id!=null) {
			if (id.startsWith(documentIdPrefix)) {
				return id.substring(documentIdPrefix.length());
			} else {
				return id;
			}
		} else {
			return null;
		}
	}

}
