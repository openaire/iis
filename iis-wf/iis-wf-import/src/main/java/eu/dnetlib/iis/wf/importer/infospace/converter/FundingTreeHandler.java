package eu.dnetlib.iis.wf.importer.infospace.converter;

import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


/**
 * Funding tree XML handler retrieving funding class details.
 * 
 * @author mhorst
 *
 */
public class FundingTreeHandler extends DefaultHandler {

    private static final String FUNDER_FUNDING_SEPARATOR = "::";
    
    private static final String ELEM_FUNDER = "funder";
    private static final String ELEM_FUNDING_LEVEL_0 = "funding_level_0";
    private static final String ELEM_NAME = "name";
    private static final String ELEM_SHORTNAME = "shortname";
    
    private Stack<String> parents;
    
    private StringBuilder currentValue;
    
    private String funderShortName;
    
    private String fundingLevel0Name;

    // ------------------------ LOGIC --------------------------
    
    @Override
    public void startDocument() throws SAXException {
        this.parents = new Stack<String>();
        this.currentValue = null;
        this.funderShortName = null;
        this.fundingLevel0Name = null;
    }

    @Override
    public void startElement(String uri, String localName, String qName,
            Attributes attributes) throws SAXException {
        if (isWithinElement(qName, ELEM_SHORTNAME, ELEM_FUNDER) || 
                isWithinElement(qName, ELEM_NAME, ELEM_FUNDING_LEVEL_0)) {
            this.currentValue = new StringBuilder();
        }
        this.parents.push(qName);
    }

    @Override
    public void endElement(String uri, String localName, String qName)
            throws SAXException {
        this.parents.pop();
        if (isWithinElement(qName, ELEM_SHORTNAME, ELEM_FUNDER)) {
            this.funderShortName = this.currentValue.toString().trim();
        } else if (isWithinElement(qName, ELEM_NAME, ELEM_FUNDING_LEVEL_0)) {
            this.fundingLevel0Name = this.currentValue.toString().trim();
        }
        this.currentValue = null;
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

    /**
     * @return funding class based of funder short name and level0 name, null returned when neither found.
     */
    public String getFundingClass() {
        StringBuilder strBuilder = new StringBuilder();
        if (funderShortName!=null) {
            strBuilder.append(funderShortName);
            strBuilder.append(FUNDER_FUNDING_SEPARATOR);
            if (fundingLevel0Name!=null) {
                strBuilder.append(fundingLevel0Name);    
            }
            return strBuilder.toString();
        } else {
            if (fundingLevel0Name!=null) {
                strBuilder.append(FUNDER_FUNDING_SEPARATOR);
                strBuilder.append(fundingLevel0Name);
                return strBuilder.toString();
            } else {
                return null;
            }
        }
    }
    
    // ------------------------ PRIVATE --------------------------
    
    private boolean isWithinElement(String qName,
            String expectedElement, String expectedParent) {
        return qName.equals(expectedElement) && 
                (expectedParent==null || !this.parents.isEmpty() && expectedParent.equals(this.parents.peek()));
    }

}

