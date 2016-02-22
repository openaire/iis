package eu.dnetlib.iis.workflows.ingest.pmc.plaintext;

import java.io.IOException;
import java.io.StringReader;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;

import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.workflows.ingest.pmc.metadata.MetadataImporter;

/**
 * @author Dominika Tkaczyk
 */

public class DocumentTextImporter
		extends Mapper<AvroKey<DocumentText>, NullWritable, AvroKey<DocumentText>, NullWritable> {

	private final Logger log = Logger.getLogger(DocumentTextImporter.class);

	Namespace oaiNamespace = null;

	@Override
	protected void setup(
			Mapper<AvroKey<DocumentText>, NullWritable, AvroKey<DocumentText>, NullWritable>.Context context)
					throws IOException, InterruptedException {
		oaiNamespace = Namespace.getNamespace(context.getConfiguration()
				.get(MetadataImporter.PARAM_INGEST_METADATA_OAI_NAMESPACE, "http://www.openarchives.org/OAI/2.0/"));
	}

	@Override
	protected void map(AvroKey<DocumentText> key, NullWritable value, Context context)
			throws IOException, InterruptedException {
		DocumentText nlm = key.datum();
		String text = null;

		if (nlm.getText() != null) {
			try {
				SAXBuilder builder = new SAXBuilder();
				builder.setValidation(false);
				builder.setFeature("http://xml.org/sax/features/validation", false);
				builder.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
				builder.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

				StringReader reader = new StringReader(nlm.getText().toString());
				Document document = builder.build(reader);
				Element sourceDocument = document.getRootElement();
				text = NlmToDocumentTextConverter.getDocumentText(sourceDocument, oaiNamespace);

			} catch (JDOMException ex) {
				log.error("Text extraction failed for id " + nlm.getId() + " :" + ex.getMessage());
			}
		}
		DocumentText output = DocumentText.newBuilder().setId(nlm.getId()).setText(text).build();
		context.write(new AvroKey<DocumentText>(output), NullWritable.get());
	}

}
