package eu.dnetlib.iis.wf.ingest.webcrawl.fundings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.InputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.junit.Before;
import org.junit.Test;

/**
 * @author mhorst
 *
 */
public class WebcrawlFundingsHandlerTest {
    
    private SAXParser saxParser;
    
    private WebcrawlFundingsHandler handler;
    
    
    @Before
    public void init() throws Exception {
        SAXParserFactory parserFactory = SAXParserFactory.newInstance();
        saxParser = parserFactory.newSAXParser();
        handler = new WebcrawlFundingsHandler();
    }

    // ----------------------- TESTS ---------------------------------
    
    @Test
    public void testParseWebcrawlWithFunding() throws Exception {
        // given
        String filePath = "/eu/dnetlib/iis/wf/ingest/webcrawl/fundings/data/wos_with_funding.xml";
        
        try (InputStream inputStream = WebcrawlFundingsHandlerTest.class.getResourceAsStream(filePath)) {
            // execute
            saxParser.parse(inputStream, handler);
        }

        // assert
        assertEquals("This work was funded by EU", handler.getFundingText());
        
    }
    
    @Test
    public void testParseWebcrawlWithoutFunding() throws Exception {
        // given
        String filePath = "/eu/dnetlib/iis/wf/ingest/webcrawl/fundings/data/wos_without_funding.xml";
        
        try (InputStream inputStream = WebcrawlFundingsHandlerTest.class.getResourceAsStream(filePath)) {
            // execute
            saxParser.parse(inputStream, handler);
        }

        // assert
        assertNull(handler.getFundingText());
        
    }
}
