package eu.dnetlib.iis.workflows.metadataextraction;

import eu.dnetlib.iis.importer.schemas.DocumentContent;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import org.jdom.Element;

/**
 * NLM {@link Element} converter building {@link DocumentContent} objects containing plaintext.
 * @author mhorst
 *
 */
public final class NlmToDocumentContentConverter {

	/**
	 * Private constructor.
	 */
	private NlmToDocumentContentConverter() {}

    /**
	 * Converts given source element to {@link DocumentContent} containing plaintext.
	 * Text might be set to null in returned object. Never returns null.
	 * @param id
	 * @param text
	 * @return {@link DocumentText}
	 */
    public static DocumentText convert(String id, String text) {
		if (id==null) {
			throw new RuntimeException("unable to set null id");
		}
		DocumentText.Builder docPlaintextBuilder = DocumentText.newBuilder();
		docPlaintextBuilder.setId(id);
		if (text!=null && !text.isEmpty()) {
			docPlaintextBuilder.setText(text);
		}
		return docPlaintextBuilder.build();
	}
    
}
