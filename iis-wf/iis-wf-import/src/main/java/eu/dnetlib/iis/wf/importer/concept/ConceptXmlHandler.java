package eu.dnetlib.iis.wf.importer.concept;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import eu.dnetlib.iis.importer.schemas.Concept;
import eu.dnetlib.iis.wf.importer.dataset.RecordReceiver;

/**
 * Context profile SAX handler.
 * Notice: writer is not being closed by handler.
 * Created outside, let it be closed outside as well.
 * @author mhorst
 *
 */
public class ConceptXmlHandler extends DefaultHandler {

	private static final String ELEM_CONCEPT = "concept";
	private static final String ELEM_PARAM = "param";
	
	private static final String ATTRIBUTE_ID = "id";
	private static final String ATTRIBUTE_LABEL = "label";
	private static final String ATTRIBUTE_NAME = "name";
	
	private Stack<String> parents;
	
	private final RecordReceiver<Concept> receiver;
	
	private StringBuilder currentValue = new StringBuilder();
	
	private Map<CharSequence,CharSequence> paramMap = null;
	
	private String currentConceptId;
	private String currentConceptLabel;
	private String currentParamName;
	
	/**
	 * Default constructor.
	 * @param receiver
	 */
	public ConceptXmlHandler(RecordReceiver<Concept> receiver) {
		super();
		this.receiver = receiver;
	}
	
	@Override
	public void startDocument() throws SAXException {
		parents = new Stack<String>();
	}

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		if (isWithinElement(qName, ELEM_CONCEPT, null)) {
			this.paramMap = new HashMap<CharSequence, CharSequence>();
			this.currentConceptId = attributes.getValue(ATTRIBUTE_ID);
			this.currentConceptLabel = attributes.getValue(ATTRIBUTE_LABEL);
		} else if (isWithinElement(qName, ELEM_PARAM, ELEM_CONCEPT)) {
			this.currentValue = new StringBuilder();
			this.currentParamName = attributes.getValue(ATTRIBUTE_NAME);
		} 
		this.parents.push(qName);
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		this.parents.pop();
		if (isWithinElement(qName, ELEM_CONCEPT, null)) {
			try {
				Concept.Builder conceptBuilder = Concept.newBuilder();
				conceptBuilder.setId(this.currentConceptId);
				conceptBuilder.setLabel(this.currentConceptLabel);
				conceptBuilder.setParams(this.paramMap);
				this.receiver.receive(conceptBuilder.build());	
			} catch (IOException e) {
				throw new SAXException(
						"Exception occurred when building concept object", e);
			}
		} else if (isWithinElement(qName, ELEM_PARAM, ELEM_CONCEPT)) {
			this.paramMap.put(currentParamName, currentValue.toString().trim());
		} 
	}

	boolean isWithinElement(String qName,
			String expectedElement, String expectedParent) {
		return qName.equalsIgnoreCase(expectedElement) && 
				(expectedParent==null || 
				(!this.parents.isEmpty() && expectedParent.equalsIgnoreCase(this.parents.peek())));
	}
	
	@Override
	public void endDocument() throws SAXException {
		parents.clear();
		parents = null;
	}

	@Override
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		if (this.currentValue!=null) {
			this.currentValue.append(ch, start, length);
		}
	}
	
}
