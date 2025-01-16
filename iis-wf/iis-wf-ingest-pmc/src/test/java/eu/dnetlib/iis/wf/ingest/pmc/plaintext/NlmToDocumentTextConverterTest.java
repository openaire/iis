package eu.dnetlib.iis.wf.ingest.pmc.plaintext;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;
import org.junit.jupiter.api.Test;

import java.io.InputStreamReader;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Dominika Tkaczyk
 * 
 */
public class NlmToDocumentTextConverterTest {

	private static final String testXML = "/eu/dnetlib/iis/wf/ingest/pmc/plaintext/document.nxml";
	private static final String testTXT = "/eu/dnetlib/iis/wf/ingest/pmc/plaintext/document.txt";

	private static final String testXmlNestedInOAI = "/eu/dnetlib/iis/wf/ingest/pmc/plaintext/document_nested_in_oai.nxml";
	private static final String testTxtNestedInOAI = "/eu/dnetlib/iis/wf/ingest/pmc/plaintext/document_nested_in_oai.txt";

    private static final String testXmlNestedInOAIWithArticleNamespace = "/eu/dnetlib/iis/wf/ingest/pmc/plaintext/document_nested_in_oai_with_article_namespace.nxml";
    private static final String testTxtNestedInOAIWithArticleNamespace = "/eu/dnetlib/iis/wf/ingest/pmc/plaintext/document_nested_in_oai_with_article_namespace.txt";

	@Test
	public void testConvertFull() throws Exception {
        SAXBuilder builder = new SAXBuilder();
        builder.setValidation(false);
        builder.setFeature("http://xml.org/sax/features/validation", false);
        builder.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
        builder.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        InputStreamReader testIS = ClassPathResourceProvider.getResourceReader(testXML);
        Document document = builder.build(testIS);
        Element sourceDocument = document.getRootElement();
        String testText = NlmToDocumentTextConverter.getDocumentText(sourceDocument, null);
        testIS.close();

		String expectedText = ClassPathResourceProvider.getResourceContent(testTXT).replaceAll(System.getProperty("line.separator"), "\n");

        assertEquals(expectedText, testText);
    }

	@Test
	public void testConvertFullNestedInOAI() throws Exception {

		SAXBuilder builder = new SAXBuilder();
		builder.setValidation(false);
		builder.setFeature("http://xml.org/sax/features/validation", false);
		builder.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
		builder.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		InputStreamReader testIS = ClassPathResourceProvider.getResourceReader(testXmlNestedInOAI);
		Document document = builder.build(testIS);
		Element sourceDocument = document.getRootElement();
		String testText = NlmToDocumentTextConverter.getDocumentText(sourceDocument,
				Namespace.getNamespace("http://www.openarchives.org/OAI/2.0/"));
		testIS.close();

		String expectedText = ClassPathResourceProvider.getResourceContent(testTxtNestedInOAI).replaceAll(System.getProperty("line.separator"), "\n");

		assertEquals(expectedText, testText);
	}

    @Test
    public void testConvertFullNestedInOAIWithArticleNamespace() throws Exception {

        SAXBuilder builder = new SAXBuilder();
        builder.setValidation(false);
        builder.setFeature("http://xml.org/sax/features/validation", false);
        builder.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
        builder.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        InputStreamReader testIS = ClassPathResourceProvider.getResourceReader(testXmlNestedInOAIWithArticleNamespace);
        Document document = builder.build(testIS);
        Element sourceDocument = document.getRootElement();
        String testText = NlmToDocumentTextConverter.getDocumentText(sourceDocument,
                Namespace.getNamespace("http://www.openarchives.org/OAI/2.0/"));
        testIS.close();

        String expectedText = ClassPathResourceProvider.getResourceContent(testTxtNestedInOAIWithArticleNamespace).replaceAll(System.getProperty("line.separator"), "\n");

        assertEquals(expectedText, testText);
    }
}
