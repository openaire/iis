package eu.dnetlib.iis.wf.importer.infospace.converter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.TreeMap;

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

    private static final String ELEM_FUNDER = "funder";
    private static final String ELEM_FUNDER_SHORTNAME = "shortname";
    private static final String ELEM_FUNDING_LEVEL_PREFIX = "funding_level_";
    private static final String ELEM_FUNDING_NAME = "name";

    
    private Stack<String> parents;
    
    private StringBuilder currentValue;
    
    private String funderShortName;
    
    private TreeMap<Integer,CharSequence> fundingLevelNames;

    // ------------------------ LOGIC --------------------------
    
    @Override
    public void startDocument() throws SAXException {
        this.parents = new Stack<String>();
        this.currentValue = null;
        this.funderShortName = null;
        this.fundingLevelNames = new TreeMap<>();
    }

    @Override
    public void startElement(String uri, String localName, String qName,
            Attributes attributes) throws SAXException {
        if (isWithinElement(qName, ELEM_FUNDER_SHORTNAME, ELEM_FUNDER) || 
                isWithinElementWithFundingLevelParent(qName, ELEM_FUNDING_NAME)) {
            this.currentValue = new StringBuilder();
        }
        this.parents.push(qName);
    }

    @Override
    public void endElement(String uri, String localName, String qName)
            throws SAXException {
        this.parents.pop();
        if (isWithinElement(qName, ELEM_FUNDER_SHORTNAME, ELEM_FUNDER)) {
            this.funderShortName = this.currentValue.toString().trim();
        } else if (isWithinElementWithFundingLevelParent(qName, ELEM_FUNDING_NAME)) {
            int fundingLevel = getFundingLevelIndexFromParentElement();
            if (fundingLevel >= 0) {
                fundingLevelNames.put(fundingLevel, this.currentValue.toString().trim());
            }
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
     * @return extracted funding tree details
     */
    public FundingDetails getFundingTreeDetails() {
        return new FundingDetails(funderShortName, convertFundingLevelNames(fundingLevelNames));
    }
    
    // ------------------------ PRIVATE --------------------------
    
    private List<CharSequence> convertFundingLevelNames(TreeMap<Integer,CharSequence> source) {
        if (source.isEmpty()) {
            return Collections.emptyList();
        }
        CharSequence[] results = new CharSequence[source.lastKey() + 1];
        for (Entry<Integer,CharSequence> entry : source.entrySet()) {
            results[entry.getKey()] = entry.getValue();
        }
        return Arrays.asList(results);
    }
    
    private boolean isWithinElement(String qName,
            String expectedElement, String expectedParent) {
        return qName.equals(expectedElement) && 
                (expectedParent==null || !this.parents.isEmpty() && expectedParent.equals(this.parents.peek()));
    }

    private boolean isWithinElementWithFundingLevelParent(String qName, String expectedElement) {
        return qName.equals(expectedElement) && !this.parents.isEmpty() && this.parents.peek().startsWith(ELEM_FUNDING_LEVEL_PREFIX);
    }
    
    private int getFundingLevelIndexFromParentElement() {
        String parentElementName = this.parents.peek();
        if (parentElementName.startsWith(ELEM_FUNDING_LEVEL_PREFIX)) {
            try {
                return Integer.parseInt(parentElementName.substring(ELEM_FUNDING_LEVEL_PREFIX.length()));    
            } catch (NumberFormatException e) {
                return -1;
            }
        } else {
            return -1;
        }
    }
}

