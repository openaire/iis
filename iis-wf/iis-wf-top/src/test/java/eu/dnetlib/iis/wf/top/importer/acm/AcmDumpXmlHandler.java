package eu.dnetlib.iis.wf.top.importer.acm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import pl.edu.icm.cermine.bibref.CRFBibReferenceParser;
import pl.edu.icm.cermine.exception.AnalysisException;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata;
import eu.dnetlib.iis.wf.importer.dataset.RecordReceiver;
import eu.dnetlib.iis.wf.metadataextraction.NlmToDocumentWithBasicMetadataConverter;

/**
 * ACM XML dump SAX handler.
 * Notice: writer is not being closed by handler.
 * Created outside, let it be closed outside as well.
 * @author mhorst
 *
 */
public class AcmDumpXmlHandler extends DefaultHandler {

	private static final String ELEM_ARTICLE_REC = "article_rec";
	private static final String ELEM_ARTICLE_ID = "article_id";
	private static final String ELEM_TITLE = "title";
	
	private static final String ELEM_REFERENCES = "references";
	private static final String ELEM_REF = "ref";
	private static final String ELEM_REF_SEQ_NO = "ref_seq_no";
	private static final String ELEM_REF_TEXT = "ref_text";
	
	private final Logger log = Logger.getLogger(this.getClass());
	
	private Stack<String> parents;
	
	private StringBuilder currentValue = new StringBuilder();
	
	private String articleId = null;
	private String title = null;
	private List<ReferenceMetadata> references = null;	
	private Integer refSeqNo = null;
	private String refText = null;
	
	private int counter = 0;
	
	private final RecordReceiver<ExtractedDocumentMetadata> receiver;
	
	private CRFBibReferenceParser bibrefParser;
	
	/**
	 * Default constructor.
	 * @param receiver
	 */
	public AcmDumpXmlHandler(RecordReceiver<ExtractedDocumentMetadata> receiver) {
		super();
		try {
			this.receiver = receiver;
			this.bibrefParser = CRFBibReferenceParser.getInstance();
		} catch (AnalysisException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void startDocument() throws SAXException {
		this.parents = new Stack<String>();
		this.counter = 0;
		clearAllFields();
	}

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		if (isWithinElement(qName, ELEM_ARTICLE_ID, ELEM_ARTICLE_REC)) {
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(qName, ELEM_TITLE, ELEM_ARTICLE_REC)) {
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(qName, ELEM_REFERENCES, ELEM_ARTICLE_REC)) {
			this.currentValue = new StringBuilder();
			this.references = new ArrayList<ReferenceMetadata>();
		} else if (isWithinElement(qName, ELEM_REF_SEQ_NO, ELEM_REF)) {
			this.currentValue = new StringBuilder();
		} else if (isWithinElement(qName, ELEM_REF_TEXT, ELEM_REF)) {
			this.currentValue = new StringBuilder();
		}
		this.parents.push(qName);
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		this.parents.pop();
		if (isWithinElement(qName, ELEM_ARTICLE_ID, ELEM_ARTICLE_REC)) {
			this.articleId = this.currentValue.toString().trim();
		} else if (isWithinElement(qName, ELEM_TITLE, ELEM_ARTICLE_REC)) {
			this.title = this.currentValue.toString().trim();
		} else if (isWithinElement(qName, ELEM_REF_SEQ_NO, ELEM_REF)) {
			this.refSeqNo = Integer.parseInt(this.currentValue.toString().trim());
		} else if (isWithinElement(qName, ELEM_REF_TEXT, ELEM_REF)) {
			this.refText = this.currentValue.toString().trim();
		} else if (isWithinElement(qName, ELEM_REF, ELEM_REFERENCES)) {
			ReferenceMetadata.Builder refMetaBuilder = ReferenceMetadata.newBuilder();
			refMetaBuilder.setPosition(this.refSeqNo);
			refMetaBuilder.setText(this.refText);
//			parsing rawText
			if (this.refText!=null && this.refText.length()>0) {
				try {
					ReferenceBasicMetadata bibrefBasicMeta = NlmToDocumentWithBasicMetadataConverter.convertBibEntry(
							bibrefParser.parseBibReference(this.refText));
					if (bibrefBasicMeta!=null) {
						refMetaBuilder.setBasicMetadata(bibrefBasicMeta);
					}
				} catch (AnalysisException e) {
					throw new SAXException("exception when parsing bibref: \n" + 
							this.refText, e);
				}	
			}
//			fallback, setting empty reference metadata when not set (null is not allowed by schema 
			if (!refMetaBuilder.hasBasicMetadata()) {
				ReferenceBasicMetadata.Builder emptyBasicMetadata = ReferenceBasicMetadata.newBuilder();
				refMetaBuilder.setBasicMetadata(emptyBasicMetadata.build());	
			}
			
			this.references.add(refMetaBuilder.build());
		} else if (isWithinElement(qName, ELEM_ARTICLE_REC, null)) {
//			writing whole record
			if (this.articleId!=null && !this.articleId.isEmpty()) {
				try {
					ExtractedDocumentMetadata.Builder docMetaBuilder = ExtractedDocumentMetadata.newBuilder();
					docMetaBuilder.setId(this.articleId);
					if (this.title!=null && !this.title.isEmpty()) {
						docMetaBuilder.setTitle(this.title);	
					}
					docMetaBuilder.setReferences(this.references);
					receiver.receive(docMetaBuilder.build());
					counter++;
					if (counter%10000==0) {
						log.debug("current progress: " + counter);
					}
				} catch (IOException e) {
					throw new SAXException(e);
				}
			} else {
				log.warn("omitting record with null/empty article id and title: " + this.title);
			}
			clearAllFields();
		}
//		resetting current value;
		this.currentValue = null;
	}

	private void clearAllFields() {
		this.articleId = null;
		this.title = null;
		this.references = null;		
		this.refSeqNo = null;
		this.refText = null;
	}
	
	boolean isWithinElement(String qName,
			String expectedElement, String expectedParent) {
		return qName.equals(expectedElement) && 
				(expectedParent==null || !this.parents.isEmpty() && expectedParent.equals(this.parents.peek()));
	}
	
	@Override
	public void endDocument() throws SAXException {
		parents.clear();
		parents = null;
		log.debug("total number of processed records: " + counter);
	}

	@Override
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		if (this.currentValue!=null) {
			this.currentValue.append(ch, start, length);
		}
	}
	
}
