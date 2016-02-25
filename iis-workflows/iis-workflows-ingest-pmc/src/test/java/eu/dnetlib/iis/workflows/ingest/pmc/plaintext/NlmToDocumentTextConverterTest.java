package eu.dnetlib.iis.workflows.ingest.pmc.plaintext;

import java.io.InputStream;

import junit.framework.TestCase;

import org.apache.commons.io.IOUtils;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;
import org.junit.Test;

import eu.dnetlib.iis.workflows.ingest.pmc.plaintext.NlmToDocumentTextConverter;

/**
 * @author Dominika Tkaczyk
 * 
 */
public class NlmToDocumentTextConverterTest extends TestCase {

	private static final String testXML = "/eu/dnetlib/iis/workflows/ingest/pmc/plaintext/document.nxml";
	private static final String testTXT = "/eu/dnetlib/iis/workflows/ingest/pmc/plaintext/document.txt";

	private static final String testXmlNestedInOAI = "/eu/dnetlib/iis/workflows/ingest/pmc/plaintext/document_nested_in_oai.nxml";
	private static final String testTxtNestedInOAI = "/eu/dnetlib/iis/workflows/ingest/pmc/plaintext/document_nested_in_oai.txt";

	@Test
	public void testConvertFull() throws Exception {

		SAXBuilder builder = new SAXBuilder();
		builder.setValidation(false);
		builder.setFeature("http://xml.org/sax/features/validation", false);
		builder.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
		builder.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		InputStream testIS = ClassLoader.class.getResourceAsStream(testXML);
		Document document = builder.build(testIS);
		Element sourceDocument = document.getRootElement();
		String testText = NlmToDocumentTextConverter.getDocumentText(sourceDocument, null);
		testIS.close();

		InputStream expectedIS = ClassLoader.class.getResourceAsStream(testTXT);
		String expectedText = IOUtils.toString(expectedIS, "UTF-8");
		expectedIS.close();

		assertEquals(expectedText, testText);
	}

	@Test
	public void testConvertFullNestedInOAI() throws Exception {

		SAXBuilder builder = new SAXBuilder();
		builder.setValidation(false);
		builder.setFeature("http://xml.org/sax/features/validation", false);
		builder.setFeature("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
		builder.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		InputStream testIS = ClassLoader.class.getResourceAsStream(testXmlNestedInOAI);
		Document document = builder.build(testIS);
		Element sourceDocument = document.getRootElement();
		String testText = NlmToDocumentTextConverter.getDocumentText(sourceDocument,
				Namespace.getNamespace("http://www.openarchives.org/OAI/2.0/"));
		testIS.close();

		InputStream expectedIS = ClassLoader.class.getResourceAsStream(testTxtNestedInOAI);
		String expectedText = IOUtils.toString(expectedIS, "UTF-8");
		expectedIS.close();

		assertEquals(expectedText, testText);
	}
}
