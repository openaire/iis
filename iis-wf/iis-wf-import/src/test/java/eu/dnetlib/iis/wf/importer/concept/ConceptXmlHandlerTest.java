package eu.dnetlib.iis.wf.importer.concept;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.importer.schemas.Concept;
import eu.dnetlib.iis.importer.schemas.Param;
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
        
        try (InputStream inputStream = ClassPathResourceProvider.getResourceInputStream(filePath)) {
            // execute
            saxParser.parse(inputStream, handler);
        }
        
        // assert
        verify(receiver, times(5)).receive(conceptCaptor.capture());  
        
        assertEquals(5, conceptCaptor.getAllValues().size());
        
        Concept concept = conceptCaptor.getAllValues().get(0);    
        assertEquals("fet-fp7::open::301::284566", concept.getId());
        assertEquals("Quantum Propagating Microwaves in Strongly Coupled Environments", concept.getLabel());
        assertEquals(3, concept.getParams().size());

        List<CharSequence> paramValues = getParamValues("CD_PROJECT_NUMBER", concept.getParams());
        assertEquals(1, paramValues.size());
        assertEquals("284566", paramValues.get(0));
        
        paramValues = getParamValues("CD_ACRONYM", concept.getParams());
        assertEquals(1, paramValues.size());
        assertEquals("PROMISCE", paramValues.get(0));
        
        paramValues = getParamValues("CD_FRAMEWORK", concept.getParams());
        assertEquals(1, paramValues.size());
        assertEquals("FP7", paramValues.get(0));
        
        concept = conceptCaptor.getAllValues().get(1);
        assertEquals("fet-fp7::open::301::284584", concept.getId());
        assertEquals("Quantum Interferometry with Bose-Einstein Condensates", concept.getLabel());
        assertEquals(2, concept.getParams().size());
        
        paramValues = getParamValues("CD_PROJECT_NUMBER", concept.getParams());
        assertEquals(1, paramValues.size());
        assertEquals("284584", paramValues.get(0));
        
        paramValues = getParamValues("CD_FRAMEWORK", concept.getParams());
        assertEquals(1, paramValues.size());
        assertEquals("FP7", paramValues.get(0));
        
        concept = conceptCaptor.getAllValues().get(2);
        assertEquals("fet-fp7::open::301", concept.getId());
        assertEquals("Challenging current thinking", concept.getLabel());
        assertEquals(5, concept.getParams().size());
        
        paramValues = getParamValues("CD_DIVNAME", concept.getParams());
        assertEquals(1, paramValues.size());
        assertEquals("7.A.SP1.03.19.01", paramValues.get(0));
        
        paramValues = getParamValues("CD_ABBR", concept.getParams());
        assertEquals(1, paramValues.size());
        assertEquals("ICT-2011.9.1", paramValues.get(0));
        
        paramValues = getParamValues("CD_PARENT_DIVNAME", concept.getParams());
        assertEquals(1, paramValues.size());
        assertEquals("7.A.SP1.03.19", paramValues.get(0));
        
        paramValues = getParamValues("DUPLICATED_ENTRY", concept.getParams());
        assertEquals(2, paramValues.size());
        assertEquals("value1", paramValues.get(0));
        assertEquals("value2", paramValues.get(1));
        
        // verifying category element
        concept = conceptCaptor.getAllValues().get(3);
        assertEquals("fet-fp7::open", concept.getId());
        assertEquals("FET Open", concept.getLabel());
        assertEquals(3, concept.getParams().size());
        
        paramValues = getParamValues("CD_DIVNAME", concept.getParams());
        assertEquals(1, paramValues.size());
        assertEquals("7.A.SP1.03.08", paramValues.get(0));
        
        paramValues = getParamValues("CD_ABBR", concept.getParams());
        assertEquals(1, paramValues.size());
        assertEquals("ICT-2007.8.0", paramValues.get(0));
        
        paramValues = getParamValues("CD_PARENT_DIVNAME", concept.getParams());
        assertEquals(1, paramValues.size());
        assertEquals("7.A.SP1.03", paramValues.get(0));
        
        // verifying context element
        concept = conceptCaptor.getAllValues().get(4);
        assertEquals("fet-fp7", concept.getId());
        assertEquals("FET", concept.getLabel());
        assertEquals(0, concept.getParams().size());
    }
    
    private List<CharSequence> getParamValues(String paramName, List<Param> params) {
        List<CharSequence> values = new ArrayList<>();
        if (params != null) {
            for (Param param : params) {
                if (paramName.equals(param.getName())) {
                    values.add(param.getValue());
                }
            }
        }
        return values;
    }
    
}
