package eu.dnetlib.iis.wf.importer.concept;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.OOZIE_ACTION_OUTPUT_FILENAME;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.file.DataFileWriter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;

import eu.dnetlib.iis.common.counter.NamedCounters;
import eu.dnetlib.iis.common.counter.NamedCountersFileWriter;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.importer.schemas.Concept;
import eu.dnetlib.iis.wf.importer.concept.model.Context;
import eu.dnetlib.iis.wf.importer.concept.model.Param;
import eu.dnetlib.iis.wf.importer.facade.ContextNotFoundException;
import eu.dnetlib.iis.wf.importer.facade.ContextStreamingFacade;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeUtils;

/**
 * RESTful endpoint based importer reading contexts provided as JSON records.  
 * @author mhorst
 *
 */
public class RestfulEndpointBasedConceptImporter implements Process {

	public static final String PARAM_IMPORT_CONTEXT_IDS_CSV = "import.context.ids.csv";
	
	protected static final String CONCEPT_COUNTER_NAME = "CONCEPT_COUNTER";
	
	private static final Logger log = Logger.getLogger(RestfulEndpointBasedConceptImporter.class);
	
	private final NamedCountersFileWriter countersWriter = new NamedCountersFileWriter();
	
	
	protected static final String PORT_OUT_CONCEPTS = "concepts";
	
	private final Map<String, PortType> outputPorts = new HashMap<String, PortType>();

	
	//------------------------ CONSTRUCTORS -------------------
	
	public RestfulEndpointBasedConceptImporter() {
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
		
		Preconditions.checkArgument(parameters.containsKey(PARAM_IMPORT_CONTEXT_IDS_CSV), 
                "unknown context identifier, required parameter '%s' is missing!", PARAM_IMPORT_CONTEXT_IDS_CSV);
		String contextIdsCSV = parameters.get(PARAM_IMPORT_CONTEXT_IDS_CSV);
		
		try (DataFileWriter<Concept> conceptWriter = getWriter(FileSystem.get(conf), portBindings)) {
            
            NamedCounters counters = new NamedCounters(new String[] { CONCEPT_COUNTER_NAME });
			
			ContextStreamingFacade streamingFacade = ServiceFacadeUtils.instantiate(parameters);
			
			String[] contextIds = StringUtils.split(contextIdsCSV, ',');
			
            for (String contextId : contextIds) {

                if (StringUtils.isNotEmpty(contextId)) {

                    try {
                        try (InputStream is = streamingFacade.getStream(contextId)) {

                            Concept[] concepts = buildConcepts(IOUtils.toString(is, StandardCharsets.UTF_8.name()));

                            for (Concept concept : concepts) {
                                conceptWriter.append(concept);
                            }

                            counters.increment(CONCEPT_COUNTER_NAME, (long) concepts.length);
                        }
                    } catch (ContextNotFoundException e) {
                        log.warn("context not found: " + contextId, e);
                    }
                }
            }
			
			countersWriter.writeCounters(counters, System.getProperty(OOZIE_ACTION_OUTPUT_FILENAME));
		}
	}
	
	/**
	 * Provides {@link Concept} writer consuming records.
	 */
	protected DataFileWriter<Concept> getWriter(FileSystem fs, PortBindings portBindings) throws IOException {
	    return DataStore.create(
                new FileSystemPath(fs, portBindings.getOutput().get(PORT_OUT_CONCEPTS)), Concept.SCHEMA$);
	}
	
	//------------------------ PRIVATE --------------------------

	/**
	 * Builds an array of concepts based on the JSON representation returned by the context endpoint.
	 * @param contextsJson contexts encoded in JSON format
	 * @return array of {@link Concept} avro records.
	 */
	private static Concept[] buildConcepts(String contextsJson) {
	    
	    return translate(new Gson().fromJson(contextsJson, Context[].class));
	}
	
	/**
	 * Translates an array of {@link IISConfigurationEntry} from the JSON model into the array of {@link Concept} objects from avro model.
	 */
	private static Concept[] translate(Context[] source) {
	    Concept[] results = new Concept[source.length];
	    for (int i=0; i < source.length; i++) {
	        results[i] = translate(source[i]);
	    }
	    return results;
	}
	
	/**
	 * Translates {@link IISConfigurationEntry} from the JSON model into the {@link Concept} object from avro model.
	 */
	private static Concept translate(Context source) {
	    Concept.Builder conceptBuilder = Concept.newBuilder();
	    conceptBuilder.setId(source.getId());
	    conceptBuilder.setLabel(source.getLabel());
	    conceptBuilder.setParams(translate(source.getParams()));
	    return conceptBuilder.build();
	}
	
	/**
	 * Translates the list of {@link Param} from the JSON model into the list of {@link eu.dnetlib.iis.importer.schemas.Param} objects from avro model.
	 */
	private static List<eu.dnetlib.iis.importer.schemas.Param> translate(List<Param> source) {
        return source.stream().map(x -> translate(x)).collect(Collectors.toList());
    }
	
	/**
	 * Translates {@link Param} from the JSON model into the {@link eu.dnetlib.iis.importer.schemas.Param} object from avro model.
	 */
	private static eu.dnetlib.iis.importer.schemas.Param translate(Param source) {
	    eu.dnetlib.iis.importer.schemas.Param.Builder paramBuilder = eu.dnetlib.iis.importer.schemas.Param.newBuilder();
	    paramBuilder.setName(source.getName());
	    paramBuilder.setValue(source.getValue());
	    return paramBuilder.build();
    }
	
}
