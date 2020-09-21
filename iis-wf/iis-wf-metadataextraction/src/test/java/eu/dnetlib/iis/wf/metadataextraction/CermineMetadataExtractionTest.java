package eu.dnetlib.iis.wf.metadataextraction;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.xpath.XPath;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

import eu.dnetlib.iis.common.IntegrationTest;
import junit.framework.TestCase;
import pl.edu.icm.cermine.ContentExtractor;
import pl.edu.icm.cermine.exception.AnalysisException;

/**
 * 
 * @author Dominika Tkaczyk
 */
@Category(IntegrationTest.class)
public class CermineMetadataExtractionTest extends TestCase {

    private static final String PDF_FILE = "/eu/dnetlib/iis/wf/metadataextraction/example-1.pdf";

	@SuppressWarnings("unchecked")
    @Test
	public void testMetadataExtraction() throws AnalysisException, IOException, JDOMException {
        
        ContentExtractor extractor = new ContentExtractor();
        
        InputStream is = ClassPathResourceProvider.getResourceInputStream(PDF_FILE);
        Element extractedContent;
        try {
            extractor.setPDF(is);
            extractedContent = extractor.getContentAsNLM();
        } finally {
            is.close();
        }
        
        Document nlm = new Document(extractedContent);
        
        assertEquals("Video Quality Prediction Models Based on Video Content Dynamics for H.264 Video over UMTS Networks",
                getElementValue(nlm, "/article/front//article-title"));
        
        Set<String> expAuthors =
                Sets.newHashSet("Asiya Khan", "Lingfen Sun", "Emmanuel Ifeachor", "Jose-Oscar Fajardo", "Fidel Liberal", "Harilaos Koumaras");
        
        XPath xPath = XPath.newInstance("/article/front//contrib-group/contrib[@contrib-type='author']/string-name");

		List<Element> nodeList = xPath.selectNodes(extractedContent);
        Set<String> authors = new HashSet<String>();
        for (Element element : nodeList) {
            authors.add(element.getText());
        }
        assertEquals(expAuthors, authors);

        Set<String> expAffiliations =
                Sets.newHashSet("1Department of Electronics and Telecommunications, University of the Basque Country (UPV/EHU), 48013 Bilbao, Spain",
                                "0Centre for Signal Processing and Multimedia Communication, School of Computing, Communications and Electronics, University of Plymouth, Plymouth PL4 8AA, UK",
                                "2Institute of Informatics and Telecommunications, NCSR Demokritos, 15310 Athens, Greece");
        
        xPath = XPath.newInstance("/article/front//contrib-group/aff");
        nodeList = xPath.selectNodes(extractedContent);
        Set<String> affiliations = new HashSet<String>();
        for (Element element : nodeList) {
            affiliations.add(element.getValue());
        }
        
        assertEquals(expAffiliations, affiliations);
        
        assertEquals("International Journal of Digital Multimedia Broadcasting",
                getElementValue(nlm, "/article/front//journal-title"));
        
        assertEquals("10.1155/2010/608138", getElementValue(nlm, "/article/front//article-id[@pub-id-type='doi']"));
        
        assertEquals("2010", getElementValue(nlm, "/article/front//pub-date/year"));
        
        assertNotNull(getElementValue(nlm, "/article/body"));
        assertFalse(getElementValue(nlm, "/article/body").isEmpty());
        
        xPath = XPath.newInstance("/article/back/ref-list/ref");
        assertEquals(32, xPath.selectNodes(extractedContent).size());
    }

    private String getElementValue(Document nlm, String xpath) throws JDOMException {
        XPath xPath = XPath.newInstance(xpath);
        String res = xPath.valueOf(nlm);
        if (res != null) {
            res = res.trim();
        }
        return res;
    }

}
