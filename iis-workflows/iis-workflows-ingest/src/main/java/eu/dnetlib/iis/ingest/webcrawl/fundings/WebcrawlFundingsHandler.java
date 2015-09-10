package eu.dnetlib.iis.ingest.webcrawl.fundings;

import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


/**
 * Webcrawl XML SAX handler providing fundings related text.
 * 
 * @author mhorst
 *
 */
public class WebcrawlFundingsHandler extends DefaultHandler {

	private static final String ELEM_CSVRECORD = "csvRecord";
	private static final String ELEM_COLUMN = "column";

	private static final String ATTR_NAME = "name";
	
	private static final String FUNDING_ATTR_VALUE = "FX";

	private Stack<String> parents;
	
	private StringBuilder currentValue;
	
	private String currentColumnName;
	
	private boolean enteredFundingColumn;
	
	private StringBuffer fundingText;
	
	@Override
	public void startDocument() throws SAXException {
		this.parents = new Stack<String>();
		this.currentValue = new StringBuilder();
		this.fundingText = new StringBuffer();
		this.enteredFundingColumn = false;
	}

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		if (isWithinElement(qName, ELEM_COLUMN, ELEM_CSVRECORD)) {
			this.currentColumnName = attributes.getValue(ATTR_NAME);
			if (FUNDING_ATTR_VALUE.equalsIgnoreCase(this.currentColumnName)) {
				this.enteredFundingColumn = true;
			}
		}
		this.parents.push(qName);
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		this.parents.pop();
		if (isWithinElement(qName, ELEM_COLUMN, ELEM_CSVRECORD) && 
				FUNDING_ATTR_VALUE.equalsIgnoreCase(this.currentColumnName)) {
			if (currentValue!=null && currentValue.length()>0) {
				if (fundingText.length()>0) {
					fundingText.append('\n');
				}
				fundingText.append(currentValue.toString());	
			}
			currentValue = new StringBuilder();
			enteredFundingColumn = false;
		} 
	}

	@Override
	public void endDocument() throws SAXException {
		parents.clear();
		parents = null;
	}

	@Override
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		if (this.enteredFundingColumn) {
			this.currentValue.append(ch, start, length);	
		}
	}
	
	boolean isWithinElement(String qName,
			String expectedElement, String expectedParent) {
		return qName.equals(expectedElement) && 
				(expectedParent==null || !this.parents.isEmpty() && expectedParent.equals(this.parents.peek()));
	}

	public CharSequence getFundingText() {
		if (fundingText.length()>0) {
			return fundingText.toString();	
		} else {
			return null;
		}
		
	}
}
