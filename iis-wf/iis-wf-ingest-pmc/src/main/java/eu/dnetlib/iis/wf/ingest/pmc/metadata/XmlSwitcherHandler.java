package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import java.util.Map;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Sax xml handler which can switch between different implementations of
 * {@link ProcessingFinishedAwareXmlHandler} content handlers.
 * Each implementation is responsible for processing only part of
 * xml enclosed by specified tag name.
 * 
 * @author madryk
 *
 */
public class XmlSwitcherHandler extends DefaultHandler {

    private ProcessingFinishedAwareXmlHandler currentHandler;
    
    private final Map<String, ProcessingFinishedAwareXmlHandler> handlers;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    /**
     * Default constructor
     * 
     * @param handlers - map of handlers responsible for parsing parts of xml.
     *      In key there should be a name of xml tag that is handled by handler specified
     *      as value.
     */
    public XmlSwitcherHandler(Map<String, ProcessingFinishedAwareXmlHandler> handlers) {
        super();
        this.currentHandler = null;
        this.handlers = handlers;
    }
    
    
    //------------------------ LOGIC --------------------------
    
    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        
        if (currentHandler == null) {
            if (handlers.containsKey(qName)) {
                currentHandler = handlers.get(qName);
                currentHandler.startDocument();
                currentHandler.startElement(uri, localName, qName, attributes);
            }
        } else {
            currentHandler.startElement(uri, localName, qName, attributes);
        }
        
    }
    
    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (currentHandler != null) {
            currentHandler.characters(ch, start, length);
        }
    }
    
    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        
        if (currentHandler != null) {
            currentHandler.endElement(uri, localName, qName);
            
            if (currentHandler.hasFinished()) {
                currentHandler.endDocument();
                currentHandler = null;
            }
        }
    }
}
