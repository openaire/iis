package eu.dnetlib.iis.wf.importer.concept;

import java.io.StringReader;
import java.security.InvalidParameterException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.avro.file.DataFileWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;

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
import eu.dnetlib.iis.wf.importer.facade.ISLookupFacade;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeUtils;

/**
 * {@link ISLookupFacade} based concept importer.
 * @author mhorst
 *
 */
public class ISLookupServiceBasedConceptImporter implements Process {

	public static final String PARAM_IMPORT_CONTEXT_IDS_CSV = "import.context.ids.csv";
	
	private static final String CONCEPT_COUNTER_NAME = "CONCEPT_COUNTER";
	
	private final Logger log = Logger.getLogger(this.getClass());
	
	private final NamedCountersFileWriter countersWriter = new NamedCountersFileWriter();
	
	
	private static final String PORT_OUT_CONCEPTS = "concepts";
	
	private static final Map<String, PortType> outputPorts = new HashMap<String, PortType>();
	
	{
		outputPorts.put(PORT_OUT_CONCEPTS, new AvroPortType(Concept.SCHEMA$));
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
		FileSystem fs = FileSystem.get(conf);
		
		String contextIdsCSV;
		if (parameters.containsKey(PARAM_IMPORT_CONTEXT_IDS_CSV)) {
			contextIdsCSV = parameters.get(PARAM_IMPORT_CONTEXT_IDS_CSV);
		} else {
			throw new InvalidParameterException("unknown context identifier");
		}
		
//		initializing ISLookup
		ISLookupFacade isLookupFacade = ServiceFacadeUtils.instantiate(parameters);

		try (DataFileWriter<Concept> conceptWriter = DataStore.create(
                new FileSystemPath(fs, portBindings.getOutput().get(PORT_OUT_CONCEPTS)), Concept.SCHEMA$)) {
            
            SAXParserFactory parserFactory = SAXParserFactory.newInstance();
            SAXParser saxParser = parserFactory.newSAXParser();
            
			NamedCounters counters = new NamedCounters(new String[] { CONCEPT_COUNTER_NAME });
			int count = 0;
			for (String contextXML : isLookupFacade.searchProfile(buildQuery(contextIdsCSV))) {
				count++;
				if (!StringUtils.isEmpty(contextXML)) {
					DataFileRecordReceiverWithCounter<Concept> conceptReciever = new DataFileRecordReceiverWithCounter<Concept>(conceptWriter);

					saxParser.parse(new InputSource(new StringReader(contextXML)), new ConceptXmlHandler(conceptReciever));
					
					counters.increment(CONCEPT_COUNTER_NAME, conceptReciever.getReceivedCount());
				} else {
					log.error("got empty context when looking for for context ids: " + contextIdsCSV);
				}
			}
			if (count==0) {
				log.warn("got 0 profiles when looking for context ids: " + contextIdsCSV);
			}
			countersWriter.writeCounters(counters, System.getProperty("oozie.action.output.properties"));
		}
	}
	
	//------------------------ PRIVATE --------------------------
	/**
	 * Builds profile lookup query for given context identifiers.
	 * @param contextIdsCSV set of context identifiers for which profiles should be found
	 */
	private static String buildQuery(String contextIdsCSV) {
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
        return query.toString();
	}
	

}
