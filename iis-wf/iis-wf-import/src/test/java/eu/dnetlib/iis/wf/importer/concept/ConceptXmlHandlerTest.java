package eu.dnetlib.iis.wf.importer.concept;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

import java.io.InputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.importer.schemas.Concept;
import eu.dnetlib.iis.wf.importer.RecordReceiver;

/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class ConceptXmlHandlerTest {
    
    private SAXParser saxParser;
    
    private ConceptXmlHandler handler;
    
    @Mock
    private RecordReceiver<Concept> receiver;
    
    @Captor
    private ArgumentCaptor<Concept> conceptCaptor;
    
    @Before
    public void init() throws Exception {
        SAXParserFactory parserFactory = SAXParserFactory.newInstance();
        saxParser = parserFactory.newSAXParser();
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
        verify(receiver, times(3)).receive(conceptCaptor.capture());  
        
        assertEquals(3, conceptCaptor.getAllValues().size());
        
        Concept concept = conceptCaptor.getAllValues().get(0);    
        assertEquals("fet-fp7::open::301::284566", concept.getId());
        assertEquals("Quantum Propagating Microwaves in Strongly Coupled Environments", concept.getLabel());
        assertEquals(3, concept.getParams().size());
        assertEquals("284566", concept.getParams().get("CD_PROJECT_NUMBER"));
        assertEquals("PROMISCE", concept.getParams().get("CD_ACRONYM"));
        assertEquals("FP7", concept.getParams().get("CD_FRAMEWORK"));
        
        concept = conceptCaptor.getAllValues().get(1);
        assertEquals("fet-fp7::open::301::284584", concept.getId());
        assertEquals("Quantum Interferometry with Bose-Einstein Condensates", concept.getLabel());
        assertEquals(2, concept.getParams().size());
        assertEquals("284584", concept.getParams().get("CD_PROJECT_NUMBER"));
        assertEquals("FP7", concept.getParams().get("CD_FRAMEWORK"));
        
        concept = conceptCaptor.getAllValues().get(2);
        assertEquals("fet-fp7::open::301", concept.getId());
        assertEquals("Challenging current thinking", concept.getLabel());
        assertEquals(3, concept.getParams().size());
        assertEquals("7.A.SP1.03.19.01", concept.getParams().get("CD_DIVNAME"));
        assertEquals("ICT-2011.9.1", concept.getParams().get("CD_ABBR"));
        assertEquals("7.A.SP1.03.19", concept.getParams().get("CD_PARENT_DIVNAME"));
        
    }
    
}
