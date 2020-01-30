package eu.dnetlib.iis.wf.ingest.springernature.plaintext;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.avro.mapred.AvroKey;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.fault.FaultUtils;
import eu.dnetlib.iis.common.javamapreduce.MultipleOutputs;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.wf.ingest.pmc.plaintext.NlmToDocumentTextConverter;

/**
 * @author mhorst
 */
public class PlaintextImporter extends Mapper<AvroKey<DocumentText>, NullWritable, NullWritable, NullWritable> {

	protected static final Logger log = Logger.getLogger(PlaintextImporter.class);

	public static final String NAMED_OUTPUT_TEXT = "output.text";
    
    public static final String NAMED_OUTPUT_FAULT = "output.fault";
    
    public static final String EXCLUDED_IDS = "excluded.ids";
	
	public static final String FAULT_TEXT = "text";

	public static final String PARAM_INGEST_METADATA_OAI_NAMESPACE = "ingest.metadata.oai.element.namespace";

	private Namespace oaiNamespace;

	/**
	 * Multiple outputs.
	 */
	private MultipleOutputs mos;

	/**
	 * Document metadata named output.
	 */
	private String namedOutputText;

	/**
	 * Fault named output.
	 */
	private String namedOutputFault;
	
	/**
     * Set of object identifiers objects excluded from processing.
     */
    private Set<String> excludedIds = Collections.emptySet();

    // ----------------------------- LOGIC ----------------------------------------
    
	@Override
	protected void setup(Mapper<AvroKey<DocumentText>, NullWritable, NullWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		namedOutputText = context.getConfiguration().get(NAMED_OUTPUT_TEXT);
		if (namedOutputText == null || namedOutputText.isEmpty()) {
			throw new RuntimeException("no named output provided for text");
		}
		namedOutputFault = context.getConfiguration().get(NAMED_OUTPUT_FAULT);
		if (namedOutputFault == null || namedOutputFault.isEmpty()) {
			throw new RuntimeException("no named output provided for fault");
		}
		mos = instantiateMultipleOutputs(context);

		oaiNamespace = Namespace.getNamespace(context.getConfiguration().get(PARAM_INGEST_METADATA_OAI_NAMESPACE,
				"http://www.openarchives.org/OAI/2.0/"));
		String excludedIdsCSV = context.getConfiguration().get(EXCLUDED_IDS);
        if (excludedIdsCSV != null && !excludedIdsCSV.trim().isEmpty()
                && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(excludedIdsCSV)) {
            log.info("got excluded ids: " + excludedIdsCSV);
            excludedIds = new HashSet<String>(Arrays.asList(StringUtils.split(excludedIdsCSV.trim(), ',')));
        } else {
            log.info("got no excluded ids");
        }
	}

	/**
     * Instantiates {@link MultipleOutputs} instance.
     */
    protected MultipleOutputs instantiateMultipleOutputs(Context context) {
        return new MultipleOutputs(context);
    }
	
	@Override
	protected void map(AvroKey<DocumentText> key, NullWritable value, Context context)
			throws IOException, InterruptedException {
		DocumentText nlm = key.datum();
		String documentId = nlm.getId().toString();
        
        if (excludedIds.contains(documentId)) {
            log.info("skipping processing for excluded id " + documentId);
            return;
        }
		
		if (!StringUtils.isBlank(nlm.getText())) {
			final DocumentText.Builder output = DocumentText.newBuilder();
			output.setId(nlm.getId());
			try {
				String pmcXml = nlm.getText().toString();
				output.setText(extractText(pmcXml, oaiNamespace));
				mos.write(namedOutputText, new AvroKey<DocumentText>(output.build()));
			} catch (Exception e) {
				handleException(nlm, e);
			}
		}
	}

	/**
	 * Extracts plain text from given xml input.
	 * 
	 * @param xmlInput
	 * @param oaiNamespace
	 * @return plaintext extracted from xml input
	 * @throws JDOMException
	 * @throws IOException
	 */
	protected static CharSequence extractText(String xmlInput, Namespace oaiNamespace)
			throws JDOMException, IOException {
		SAXBuilder builder = new SAXBuilder();
		builder.setValidation(false);
		builder.setFeature("http://xml.org/sax/features/validation", false);
		builder.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
		builder.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		StringReader textReader = new StringReader(xmlInput);
		Document document = builder.build(textReader);
		Element sourceDocument = document.getRootElement();
		return NlmToDocumentTextConverter.getDocumentText(sourceDocument, oaiNamespace);
	}

	/**
	 * Handles exception by writing it as fault.
	 * 
	 * @param documentText source xml content
	 * @param e thrown exception
	 * @param builder {@link ExtractedDocumentMetadata} builder
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void handleException(DocumentText documentText, Exception e) throws IOException, InterruptedException {
        // writing fault result
		Map<CharSequence, CharSequence> auditSupplementaryData = new HashMap<CharSequence, CharSequence>();
		auditSupplementaryData.put(FAULT_TEXT, documentText.getText());
		mos.write(namedOutputFault,
				new AvroKey<Fault>(FaultUtils.exceptionToFault(documentText.getId(), e, auditSupplementaryData)));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.
	 * Mapper.Context)
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		log.debug("cleanup: closing multiple outputs...");
		mos.close();
		log.debug("cleanup: multiple outputs closed");
	}

}
