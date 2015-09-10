package eu.dnetlib.iis.importer.vocabulary;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.lang.StringUtils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Vocabulary profile SAX handler.
 * Builds map with codes as keys and english names as values.
 * Notice: writer is not being closed by handler.
 * Created outside, let it be closed outside as well.
 * @author mhorst
 *
 */
public class VocabularyXmlHandler extends DefaultHandler {

	private static final String ELEM_TERM = "term";
	
	private static final String ATTRIBUTE_ENGLISH_NAME = "english_name";
	private static final String ATTRIBUTE_CODE = "code";
	
	private Stack<String> parents;
	
	private Map<String,String> vocabularyMap = null;
	
	@Override
	public void startDocument() throws SAXException {
		parents = new Stack<String>();
		vocabularyMap = new HashMap<String, String>();
	}

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		if (isWithinElement(qName, ELEM_TERM, null)) {
			String code = attributes.getValue(ATTRIBUTE_CODE);
			String name = attributes.getValue(ATTRIBUTE_ENGLISH_NAME);
			if (!StringUtils.isEmpty(code) && !StringUtils.isEmpty(name)) {
				vocabularyMap.put(code, name);
			}
		}
		this.parents.push(qName);
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		this.parents.pop();
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

	public Map<String, String> getVocabularyMap() {
		return vocabularyMap;
	}

}
