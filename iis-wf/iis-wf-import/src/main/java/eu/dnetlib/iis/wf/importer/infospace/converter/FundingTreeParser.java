package eu.dnetlib.iis.wf.importer.infospace.converter;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 * Parser module processing funding tree XML.
 * @author mhorst
 *
 */
public class FundingTreeParser {

    private SAXParser saxParser;
    
    // ------------------------ CONSTRUCTORS --------------------------
    
    public FundingTreeParser () {
        try {
            SAXParserFactory saxFactory = SAXParserFactory.newInstance();
            saxFactory.setValidating(false);
            saxParser = saxFactory.newSAXParser();
            XMLReader reader = saxParser.getXMLReader();
            reader.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
            reader.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            reader.setFeature("http://xml.org/sax/features/validation", false);
        } catch (ParserConfigurationException | SAXException e) {
            throw new RuntimeException("Error occurred while initializing SAX parser!", e);
        }
    }
    
    // ------------------------ LOGIC --------------------------
    
    /**
     * Extracts funding details out of the funding tree.
     * @param fundingTreeList list of source funding tree XMLs to be parsed
     */
    public FundingDetails extractFundingDetails(List<String> fundingTreeList) throws IOException {
        if (CollectionUtils.isNotEmpty(fundingTreeList)) {
            for (String fundingTreeXML : fundingTreeList) {
                if (StringUtils.isNotBlank(fundingTreeXML)) {
                    FundingDetails result = extractFundingDetails(fundingTreeXML);
                   if (result!=null) {
                       return result;
                   }
                }
            }
        }
        // fallback
        return null;
    }
    
    /**
     * Extracts funding details out of the funding tree.
     * @param fundingTreeXML source funding tree XML to be parsed
     */
    public FundingDetails extractFundingDetails(String fundingTreeXML) throws IOException {
        try {
            FundingTreeHandler handler = new FundingTreeHandler();
            saxParser.parse(new InputSource(new StringReader(fundingTreeXML)), handler);
            return handler.getFundingTreeDetails();
        } catch (SAXException e) {
            throw new IOException("exception occurred when processing xml: " + fundingTreeXML, e);
        }
    }
    
    
}
