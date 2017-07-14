package eu.dnetlib.iis.wf.importer.mdrecord;

import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import eu.dnetlib.iis.common.InfoSpaceConstants;

/**
 * MDRecord handler extracting record identifier.
 * 
 * Notice: writer is not being closed by handler. Created outside, let it be closed outside as well.
 * @author mhorst
 *
 */
public class MDRecordHandler extends DefaultHandler {

    public static final String ELEM_OBJ_IDENTIFIER = "objIdentifier";
    
    private static final String ELEM_HEADER = "header";
    
    private Stack<String> parents;
    
    private StringBuilder currentValue = new StringBuilder();
    
    private String recordId;
    
    
    // ------------------------ LOGIC --------------------------
    
    @Override
    public void startDocument() throws SAXException {
        parents = new Stack<String>();
        recordId = null;
    }

    @Override
    public void startElement(String uri, String localName, String qName,
            Attributes attributes) throws SAXException {
        if (this.recordId == null) {
            if (isWithinElement(localName, ELEM_OBJ_IDENTIFIER, ELEM_HEADER)) {
//              identifierType attribute is mandatory
                this.currentValue = new StringBuilder();
            }
            this.parents.push(localName);            
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName)
            throws SAXException {
        if (this.recordId == null) {
            this.parents.pop();
            if (isWithinElement(localName, ELEM_OBJ_IDENTIFIER, ELEM_HEADER)) {
                this.recordId = InfoSpaceConstants.ROW_PREFIX_RESULT + this.currentValue.toString().trim();
            }
//          resetting current value;
            this.currentValue = null;
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
        if (this.currentValue!=null) {
            this.currentValue.append(ch, start, length);
        }
    }

    /**
     * @return record identifier
     */
    public String getRecordId() {
        return recordId;
    }
    
    // ------------------------ PRIVATE --------------------------
    
    private boolean isWithinElement(String localName, String expectedElement, String expectedParent) {
        return localName.equals(expectedElement) && !this.parents.isEmpty() && 
                expectedParent.equals(this.parents.peek());
    }

    
}

