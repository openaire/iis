package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.*;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.TagHierarchyUtils.*;

import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata;

/**
 * Sax xml handler of &lt;journal-meta&gt; tag in JATS xml
 * 
 * @author mhorst
 * @author madryk
 *
 */
public class JournalMetaXmlHandler extends DefaultHandler implements ProcessingFinishedAwareXmlHandler {
    
    private Stack<String> parents;
    
    private final ExtractedDocumentMetadata.Builder builder;
    
    private String currentValue;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    public JournalMetaXmlHandler(ExtractedDocumentMetadata.Builder builder) {
        this.builder = builder;
        this.parents = new Stack<String>();
    }
    
    
    //------------------------ LOGIC --------------------------
    
    @Override
    public void startDocument() throws SAXException {
        this.parents = new Stack<String>();
    }
    
    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        
        
        this.parents.push(qName);
    }
    
    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        this.parents.pop();
        
        if (isElement(qName, ELEM_JOURNAL_TITLE)) {
            if (!builder.hasJournal()) {
                // taking only first journal title into account when more than one specified
                builder.setJournal(this.currentValue.toString().trim());
            }
        }
        
    }
    
    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        
        this.currentValue = new String(ch, start, length);
        
    }

    @Override
    public boolean hasFinished() {
        return parents.isEmpty();
    }
    
    
    
}
