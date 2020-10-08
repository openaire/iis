package eu.dnetlib.iis.wf.ingest.webcrawl.fundings;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author mhorst
 *
 */
public class WebcrawlFundingsHandlerTest {
    
    private SAXParser saxParser;
    
    private WebcrawlFundingsHandler handler;
    
    
    @BeforeEach
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

        try (InputStream inputStream = ClassPathResourceProvider.getResourceInputStream(filePath)) {
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
        
        try (InputStream inputStream = ClassPathResourceProvider.getResourceInputStream(filePath)) {
            // execute
            saxParser.parse(inputStream, handler);
        }

        // assert
        assertNull(handler.getFundingText());
        
    }
}
