package eu.dnetlib.iis.workflows.ingest.pmc.metadata;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

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
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.fault.FaultUtils;
import eu.dnetlib.iis.common.javamapreduce.MultipleOutputs;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.workflows.ingest.pmc.plaintext.NlmToDocumentTextConverter;

/**
 * @author Michal Oniszczuk (m.oniszczuk@icm.edu.pl)
 * @author mhorst
 */
public class MetadataImporter extends Mapper<AvroKey<DocumentText>, NullWritable, NullWritable, NullWritable> {

	protected final Logger log = Logger.getLogger(this.getClass());

	public static final String FAULT_TEXT = "text";

	public static final String PARAM_INGEST_METADATA = "ingest.metadata";

	public static final String PARAM_INGEST_METADATA_OAI_NAMESPACE = "ingest.metadata.oai.element.namespace";

	boolean ingestMetadata = true;

	Namespace oaiNamespace = null;

	/**
	 * Multiple outputs.
	 */
	protected MultipleOutputs mos = null;

	/**
	 * Document metadata named output.
	 */
	protected String namedOutputMeta;

	/**
	 * Fault named output.
	 */
	protected String namedOutputFault;

	@Override
	protected void setup(Mapper<AvroKey<DocumentText>, NullWritable, NullWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		namedOutputMeta = context.getConfiguration().get("output.meta");
		if (namedOutputMeta == null || namedOutputMeta.isEmpty()) {
			throw new RuntimeException("no named output provided for metadata");
		}
		namedOutputFault = context.getConfiguration().get("output.fault");
		if (namedOutputFault == null || namedOutputFault.isEmpty()) {
			throw new RuntimeException("no named output provided for fault");
		}
		mos = new MultipleOutputs(context);

		String ingestMeta = context.getConfiguration().get(PARAM_INGEST_METADATA);
		if (ingestMeta != null) {
			ingestMetadata = Boolean.valueOf(ingestMeta);
		}

		oaiNamespace = Namespace.getNamespace(context.getConfiguration().get(PARAM_INGEST_METADATA_OAI_NAMESPACE,
				"http://www.openarchives.org/OAI/2.0/"));
	}

	@Override
	protected void map(AvroKey<DocumentText> key, NullWritable value, Context context)
			throws IOException, InterruptedException {
		DocumentText nlm = key.datum();
		if (!StringUtils.isBlank(nlm.getText())) {
			final ExtractedDocumentMetadata.Builder output = ExtractedDocumentMetadata.newBuilder();
			output.setId(nlm.getId());
			try {
				String pmcXml = nlm.getText().toString();
				output.setText(extractText(pmcXml, oaiNamespace));
				if (ingestMetadata) {
					extractMetadata(pmcXml, output);
				} else {
					// setting required field
					output.setEntityType("");
				}
				mos.write(namedOutputMeta, new AvroKey<ExtractedDocumentMetadata>(output.build()));
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
	 * Extracts metadata from given xml input by supplementing metada in output
	 * builder.
	 * 
	 * @param xmlInput
	 * @param output
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 */
	protected static void extractMetadata(String xmlInput, ExtractedDocumentMetadata.Builder output)
			throws ParserConfigurationException, SAXException, IOException {
		SAXParserFactory saxFactory = SAXParserFactory.newInstance();
		saxFactory.setValidating(false);
		SAXParser saxParser = saxFactory.newSAXParser();
		XMLReader reader = saxParser.getXMLReader();
		reader.setFeature("http://xml.org/sax/features/validation", false);
		reader.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
		reader.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		PmcXmlHandler pmcXmlHandler = new PmcXmlHandler(output);
		saxParser.parse(new InputSource(new StringReader(xmlInput)), pmcXmlHandler);
	}

	/**
	 * Handles exception by writing it as fault.
	 * 
	 * @param documentText
	 * @param e
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void handleException(DocumentText documentText, Exception e) throws IOException, InterruptedException {
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
