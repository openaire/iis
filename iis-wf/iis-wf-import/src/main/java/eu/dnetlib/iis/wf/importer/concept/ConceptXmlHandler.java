package eu.dnetlib.iis.wf.importer.concept;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.importer.schemas.Concept;
import eu.dnetlib.iis.importer.schemas.Param;
import eu.dnetlib.iis.wf.importer.RecordReceiver;

/**
 * Context profile SAX handler. Builds {@link Concept} objects based on XML profile with concepts hierarchy.
 * 
 * @author mhorst
 *
 */
public class ConceptXmlHandler extends DefaultHandler {

	private static final String ELEM_CONTEXT = "context";
	private static final String ELEM_CATEGORY = "category";
    private static final String ELEM_CONCEPT = "concept";
    private static final String ELEM_PARAM = "param";

    private static final String ATTRIBUTE_ID = "id";
    private static final String ATTRIBUTE_LABEL = "label";
    private static final String ATTRIBUTE_NAME = "name";

    private Stack<String> parentElementNames;
    
    private Stack<Concept.Builder> parentConcepts;

    private final RecordReceiver<Concept> receiver;

    private StringBuilder currentValue = new StringBuilder();

    private String currentParamName;
    
    private Concept.Builder currentConceptBuilder;

    //-------------------- CONSTRUCTORS -------------------------
    
    /**
     * @param receiver record receiver
     */
    public ConceptXmlHandler(RecordReceiver<Concept> receiver) {
        super();
        this.receiver = receiver;
    }

    //-------------------- LOGIC --------------------------------
    
    @Override
    public void startDocument() throws SAXException {
        parentElementNames = new Stack<>();
        parentConcepts = new Stack<>();
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        if (isConceptElement(qName)) {
            if (currentConceptBuilder!=null) {
                parentConcepts.push(currentConceptBuilder);
            }
            currentConceptBuilder = Concept.newBuilder();
            currentConceptBuilder.setId(attributes.getValue(ATTRIBUTE_ID));
            currentConceptBuilder.setLabel(attributes.getValue(ATTRIBUTE_LABEL));

        } else if (isParamElement(qName)) {
            currentValue = new StringBuilder();
            currentParamName = attributes.getValue(ATTRIBUTE_NAME);
        }
        parentElementNames.push(qName);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        this.parentElementNames.pop();
        if (isConceptElement(qName)) {
            try {
                if (currentConceptBuilder==null) {
                    currentConceptBuilder = parentConcepts.pop();
                }
                if (!currentConceptBuilder.hasParams()) {
                	currentConceptBuilder.setParams(Lists.newArrayList());
                }
                receiver.receive(currentConceptBuilder.build());
                currentConceptBuilder = null;
            } catch (IOException e) {
                throw new SAXException("Exception occurred when building concept object", e);
            }
        } else if (isParamElement(qName)) {
            if (currentConceptBuilder==null) {
                currentConceptBuilder = parentConcepts.pop();
            }
            if (!currentConceptBuilder.hasParams()) {
                currentConceptBuilder.setParams(new ArrayList<>());
            }
            Param.Builder paramBuilder = Param.newBuilder();
            paramBuilder.setName(currentParamName);
            paramBuilder.setValue(currentValue.toString().trim());
            currentConceptBuilder.getParams().add(paramBuilder.build());
        }
    }

    @Override
    public void endDocument() throws SAXException {
        parentElementNames.clear();
        parentElementNames = null;
        parentConcepts.clear();
        parentConcepts = null;
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (currentValue != null) {
            currentValue.append(ch, start, length);
        }
    }

    //-------------------- PRIVATE --------------------------------
    
    private boolean isParamElement(String qName) {
    	return isWithinElement(qName, ELEM_PARAM);
    }
    
    private boolean isConceptElement(String qName) {
    	return isWithinElement(qName, ELEM_CONCEPT) || 
    			isWithinElement(qName, ELEM_CONTEXT) || 
    			isWithinElement(qName, ELEM_CATEGORY);
    }
    
    /**
     * Verifies position in XML tree by checking current element and optionally its parrent.
     * @param qName current element name
     * @param expectedElement expected element name
     */
    private boolean isWithinElement(String qName, String expectedElement) {
        return qName.equalsIgnoreCase(expectedElement);
    }
    
}
