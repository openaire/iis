package eu.dnetlib.iis.wf.importer.vocabulary;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_ISLOOKUP_SERVICE_LOCATION;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_VOCABULARY_CODE;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_VOCABULARY_OUTPUT_FILENAME;

import java.io.StringReader;
import java.security.InvalidParameterException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.ws.wsaddressing.W3CEndpointReferenceBuilder;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.xml.sax.InputSource;

import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.porttype.AnyPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * {@link ISLookUpService} based vocabulary importer.
 * @author mhorst
 *
 */
public class ISLookupServiceBasedVocabularyImporter implements Process {

	private static final String DEFAULT_VOCABULARY_CODE = "dnet:countries";
	
	private static final String PORT_OUT_VOCABULARY = "vocabulary";
	
	private static final Map<String, PortType> outputPorts = new HashMap<String, PortType>();
	
	{
		outputPorts.put(PORT_OUT_VOCABULARY, new AnyPortType());
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
		if (!parameters.containsKey(IMPORT_VOCABULARY_OUTPUT_FILENAME)) {
			throw new InvalidParameterException("unknown output filename");
		}
		String vocabularyCode = DEFAULT_VOCABULARY_CODE;
		if (parameters.containsKey(IMPORT_VOCABULARY_CODE)) {
			vocabularyCode = parameters.get(IMPORT_VOCABULARY_CODE);
		}
		
		W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
		String isLookupServiceLocation = parameters.get(IMPORT_ISLOOKUP_SERVICE_LOCATION);
		eprBuilder.address(isLookupServiceLocation);
		eprBuilder.build();
		ISLookUpService isLookupService = new JaxwsServiceResolverImpl().getService(
				ISLookUpService.class, eprBuilder.build());
		String vocabularyXML = isLookupService.getResourceProfileByQuery("//BODY/CONFIGURATION/TERMS"
				+ "[../VOCABULARY_NAME/@code=\""+vocabularyCode+"\"]");
		if (StringUtils.isEmpty(vocabularyXML)) {
			throw new RuntimeException("got empty vocabulary for code: " + 
					vocabularyCode + ", service location: " + isLookupServiceLocation);
		}
		VocabularyXmlHandler handler = new VocabularyXmlHandler();
		SAXParserFactory parserFactory = SAXParserFactory.newInstance();
		SAXParser saxParser = parserFactory.newSAXParser();
		saxParser.parse(
				new InputSource(new StringReader(vocabularyXML)), 
				handler);
		Properties properties = new Properties();
		properties.putAll(handler.getVocabularyMap());
		
		FSDataOutputStream outputStream = fs.create(
				new Path(
						portBindings.getOutput().get(PORT_OUT_VOCABULARY), 
						parameters.get(IMPORT_VOCABULARY_OUTPUT_FILENAME)), 
				true);
		try {
			properties.store(outputStream, null);	
		} finally {
			outputStream.close();
		}
	}

}
