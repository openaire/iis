package eu.dnetlib.iis.wf.importer.concept;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_ISLOOKUP_SERVICE_LOCATION;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT;

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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;

import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpDocumentNotFoundException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.enabling.resultset.client.ResultSetClientFactory;
import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;
import eu.dnetlib.iis.common.counter.NamedCounters;
import eu.dnetlib.iis.common.counter.NamedCountersFileWriter;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.importer.schemas.Concept;
import eu.dnetlib.iis.wf.importer.DataFileRecordReceiverWithCounter;

/**
 * {@link ISLookUpService} based concept importer.
 * @author mhorst
 *
 */
public class ISLookupServiceBasedConceptImporter implements Process {

	public static final String PARAM_IMPORT_CONTEXT_IDS_CSV = "import.context.ids.csv";
	
	public static final String PARAM_IMPORT_RESULTSET_PAGESIZE = "import.resultset.pagesize";
	
	private static final String CONCEPT_COUNTER_NAME = "CONCEPT_COUNTER";
	
	private final Logger log = Logger.getLogger(this.getClass());
	
	private final int defaultPagesize = 100;
	
	private final NamedCountersFileWriter countersWriter = new NamedCountersFileWriter();
	
	
	private static final String PORT_OUT_CONCEPTS = "concepts";
	
	private static final Map<String, PortType> outputPorts = new HashMap<String, PortType>();
	
	{
		outputPorts.put(PORT_OUT_CONCEPTS, new AvroPortType(Concept.SCHEMA$));
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
		if (!parameters.containsKey(IMPORT_ISLOOKUP_SERVICE_LOCATION)) {
			throw new InvalidParameterException("unknown ISLookup service location, "
					+ "required parameter '" + IMPORT_ISLOOKUP_SERVICE_LOCATION + "' is missing!");
		}
		String contextIdsCSV;
		if (parameters.containsKey(PARAM_IMPORT_CONTEXT_IDS_CSV)) {
			contextIdsCSV = parameters.get(PARAM_IMPORT_CONTEXT_IDS_CSV);
		} else {
			throw new InvalidParameterException("unknown context identifier");
		}
		
//		setting result set client read timeout
		Long rsClientReadTimeout = null;
		if (parameters.containsKey(IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT)) {
			rsClientReadTimeout = Long.valueOf(
					parameters.get(IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT));
		}
		
		W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
		String isLookupServiceLocation = parameters.get(IMPORT_ISLOOKUP_SERVICE_LOCATION);
		eprBuilder.address(isLookupServiceLocation);
		eprBuilder.build();
		ISLookUpService isLookupService = new JaxwsServiceResolverImpl().getService(
				ISLookUpService.class, eprBuilder.build());
		
		DataFileWriter<Concept> conceptWriter = null;
		try {
//			initializing avro datastore writer
			conceptWriter = DataStore.create(
					new FileSystemPath(fs, portBindings.getOutput().get(PORT_OUT_CONCEPTS)), 
					Concept.SCHEMA$);
			
			String[] contextIds = StringUtils.split(contextIdsCSV, ',');
			if (contextIds.length==0) {
				throw new RuntimeException("got 0 context ids, "
						+ "unable to tokenize context identifiers: " + contextIdsCSV);
			}
			StringBuilder query = new StringBuilder("//BODY/CONFIGURATION/context[");
			int tokensCount = 0;
			for (String contextId : contextIds) {
				if (tokensCount>0) {
					query.append(" or ");		
				}
				query.append("@id=\"" + contextId + "\"");	
				tokensCount++;
			}
			query.append(']');
			
			W3CEndpointReference results = isLookupService.searchProfile(
					query.toString());
			ResultSetClientFactory rsFactory = new ResultSetClientFactory();
			if (rsClientReadTimeout!=null) {
				rsFactory.setTimeout(rsClientReadTimeout);	
			}
			rsFactory.setServiceResolver(new JaxwsServiceResolverImpl());
			rsFactory.setPageSize(parameters.containsKey(PARAM_IMPORT_RESULTSET_PAGESIZE)?
							Integer.valueOf(parameters.get(PARAM_IMPORT_RESULTSET_PAGESIZE)):
								defaultPagesize);
//			supporting multiple profiles
			NamedCounters counters = new NamedCounters(new String[] { CONCEPT_COUNTER_NAME });
			int count = 0;
			for (String contextXML : rsFactory.getClient(results)) {
				count++;
				if (!StringUtils.isEmpty(contextXML)) {
					DataFileRecordReceiverWithCounter<Concept> conceptReciever = new DataFileRecordReceiverWithCounter<Concept>(conceptWriter);
					
					SAXParserFactory parserFactory = SAXParserFactory.newInstance();
					SAXParser saxParser = parserFactory.newSAXParser();
					saxParser.parse(
							new InputSource(new StringReader(contextXML)), 
							new ConceptXmlHandler(conceptReciever));
					
					counters.increment(CONCEPT_COUNTER_NAME, conceptReciever.getReceivedCount());
				} else {
					log.error("got empty context when looking for for context ids: " + 
							contextIdsCSV + ", service location: " + isLookupServiceLocation);
				}
			}
			if (count==0) {
				log.warn("got 0 profiles when looking for context ids: " + 
						contextIdsCSV + ", service location: " + isLookupServiceLocation);
			}
			countersWriter.writeCounters(counters, System.getProperty("oozie.action.output.properties"));
		} catch (ISLookUpDocumentNotFoundException e) {
			log.error("unable to find profile for context ids: " + 
					contextIdsCSV + ", service location: " + isLookupServiceLocation, e);
		} finally {
			if (conceptWriter!=null) {
				conceptWriter.close();	
			}	
		}
	}

}
