package eu.dnetlib.iis.wf.importer.concept;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.junit.Before;
import org.junit.Test;

import eu.dnetlib.iis.importer.schemas.Concept;
import eu.dnetlib.iis.wf.importer.RecordReceiver;

/**
 * @author mhorst
 *
 */
public class ConceptXmlHandlerTest {
    
    private SAXParser saxParser;
    
    private ConceptsReceiver receiver;
    
    private ConceptXmlHandler handler;
    
    
    @Before
    public void init() throws Exception {
        SAXParserFactory parserFactory = SAXParserFactory.newInstance();
        saxParser = parserFactory.newSAXParser();
        receiver = new ConceptsReceiver();
        handler = new ConceptXmlHandler(receiver);
    }

    // ----------------------- TESTS ---------------------------------
    
    @Test
    public void testParse() throws Exception {
        
        // given
        String filePath = "/eu/dnetlib/iis/wf/importer/concept/data/input/fet-fp7.xml";
        
        try (InputStream inputStream = ConceptXmlHandlerTest.class.getResourceAsStream(filePath)) {
            // execute
            saxParser.parse(inputStream, handler);
        }
        
        // assert
        assertEquals(3, receiver.getConcepts().size());
        
        assertEquals("fet-fp7::open::301::284566", receiver.getConcepts().get(0).getId());
        assertEquals("Quantum Propagating Microwaves in Strongly Coupled Environments", receiver.getConcepts().get(0).getLabel());
        assertEquals(3, receiver.getConcepts().get(0).getParams().size());
        assertEquals("284566", receiver.getConcepts().get(0).getParams().get("CD_PROJECT_NUMBER"));
        assertEquals("PROMISCE", receiver.getConcepts().get(0).getParams().get("CD_ACRONYM"));
        assertEquals("FP7", receiver.getConcepts().get(0).getParams().get("CD_FRAMEWORK"));
        
        assertEquals("fet-fp7::open::301::284584", receiver.getConcepts().get(1).getId());
        assertEquals("Quantum Interferometry with Bose-Einstein Condensates", receiver.getConcepts().get(1).getLabel());
        assertEquals(2, receiver.getConcepts().get(1).getParams().size());
        assertEquals("284584", receiver.getConcepts().get(1).getParams().get("CD_PROJECT_NUMBER"));
        assertEquals("FP7", receiver.getConcepts().get(1).getParams().get("CD_FRAMEWORK"));
        
        assertEquals("fet-fp7::open::301", receiver.getConcepts().get(2).getId());
        assertEquals("Challenging current thinking", receiver.getConcepts().get(2).getLabel());
        assertEquals(3, receiver.getConcepts().get(2).getParams().size());
        assertEquals("7.A.SP1.03.19.01", receiver.getConcepts().get(2).getParams().get("CD_DIVNAME"));
        assertEquals("ICT-2011.9.1", receiver.getConcepts().get(2).getParams().get("CD_ABBR"));
        assertEquals("7.A.SP1.03.19", receiver.getConcepts().get(2).getParams().get("CD_PARENT_DIVNAME"));
        
    }
    
 //------------------------ INNER CLASS --------------------------
    
    private static class ConceptsReceiver implements RecordReceiver<Concept> {
        
        private final List<Concept> concepts = new ArrayList<Concept>();

        //------------------------ GETTERS --------------------------
        
        public List<Concept> getConcepts() {
            return concepts;
        }
        
        //------------------------ LOGIC --------------------------
        @Override
        public void receive(Concept object) throws IOException {
            concepts.add(object);
            
        }
    }
    
}
