package eu.dnetlib.iis.wf.referenceextraction.patent.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.StringWriter;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;

/**
 * {@link OpsPatentMetadataXPathBasedParser} test class.
 * 
 * @author mhorst
 *
 */
public class OpsPatentMetadataXPathBasedParserTest {

    static final String xmlResourcesRootClassPath = "/eu/dnetlib/iis/wf/referenceextraction/patent/data/";

    private OpsPatentMetadataXPathBasedParser parser = new OpsPatentMetadataXPathBasedParser();


    // -------------------------------- TESTS ----------------------------
    
    @Test
    public void testExtractMetadataFromValidXMLfile() throws Exception {
        // given
        String xmlContents = ClassPathResourceProvider
                .getResourceContent(xmlResourcesRootClassPath + "WO.0042078.A1.xml");

        // execute
        Patent.Builder patent = parser.parse(xmlContents, getPatentBuilderInitializedWithRequiredFields());

        // assert
        assertNotNull(patent);
        assertTrue(patent.hasApplnTitle());
        assertEquals("AQUEOUS PEROXIDE EMULSIONS", patent.getApplnTitle());
        assertNotNull(patent.getApplnAbstract());
        assertEquals("Simple abstract in English", patent.getApplnAbstract());
        assertNotNull(patent.getIpcClassSymbol());
        assertEquals(1, patent.getIpcClassSymbol().size());
        assertEquals("C08F2/20", patent.getIpcClassSymbol().get(0));
        assertEquals("WO2000EP00003", patent.getApplnNrEpodoc());
        assertEquals("2000-01-06", patent.getApplnFilingDate());
        assertEquals("2000-07-20", patent.getEarliestPublnDate());

        assertNotNull(patent.getApplicantNames());
        assertEquals(3, patent.getApplicantNames().size());
        assertEquals("AKZO NOBEL NV", patent.getApplicantNames().get(0));
        assertEquals("WESTMIJZE HANS", patent.getApplicantNames().get(1));
        assertEquals("BOEN HO O", patent.getApplicantNames().get(2));

        assertNotNull(patent.getApplicantCountryCodes());
        assertEquals(3, patent.getApplicantCountryCodes().size());
        assertEquals("NL", patent.getApplicantCountryCodes().get(0));
        assertEquals("NL", patent.getApplicantCountryCodes().get(1));
        assertEquals("NL", patent.getApplicantCountryCodes().get(2));
    }
    
    @Test
    public void testExtractFromDocumentWithAllFieldsMissing() throws Exception {
        // given
        String xmlContents = buildXMLContents(null, null, null, null, null, null, null);

        // execute
        Patent.Builder patentBuilder = parser.parse(xmlContents, getPatentBuilderInitializedWithRequiredFields());

        // assert
        Patent patent = patentBuilder.build();
        assertNotNull(patent);
        assertNull(patent.getApplnTitle());
        assertNull(patent.getApplnAbstract());
        assertNull(patent.getApplnNrEpodoc());
        assertNull(patent.getApplnFilingDate());
        assertNull(patent.getEarliestPublnDate());
        assertNull(patent.getIpcClassSymbol());
    }
    
    @Test
    public void testExtractFromDocumentWithAllFieldsSet() throws Exception {
        // given
        String title = "some title";
        String abstractText = "some abstract";
        String classIpcText = "some class IPC text";
        String applnEpodocId = "1234";
        String applnDate = "20200501";
        String publnDate = "20000530";
        List<String> epodocApplicantNames = Lists.newArrayList("John Smith [PL]");
        
        String xmlContents = buildXMLContents(title, abstractText, classIpcText, applnEpodocId, applnDate, publnDate,
                epodocApplicantNames);

        // execute
        Patent.Builder patentBuilder = parser.parse(xmlContents, getPatentBuilderInitializedWithRequiredFields());

        // assert
        Patent patent = patentBuilder.build();
        assertNotNull(patent);
        assertEquals(title, patent.getApplnTitle());
        assertNotNull(patent.getApplnAbstract());
        assertEquals(abstractText, patent.getApplnAbstract());
        assertNotNull(patent.getIpcClassSymbol());
        assertEquals(1, patent.getIpcClassSymbol().size());
        assertEquals(classIpcText, patent.getIpcClassSymbol().get(0));
        assertEquals(applnEpodocId, patent.getApplnNrEpodoc());
        assertEquals("2020-05-01", patent.getApplnFilingDate());
        assertEquals("2000-05-30", patent.getEarliestPublnDate());

        assertNotNull(patent.getApplicantNames());
        assertEquals(1, patent.getApplicantNames().size());
        assertEquals("John Smith", patent.getApplicantNames().get(0));

        assertNotNull(patent.getApplicantCountryCodes());
        assertEquals(1, patent.getApplicantCountryCodes().size());
        assertEquals("PL", patent.getApplicantCountryCodes().get(0));
    }

    @Test
    public void testExtractFromDocumentWithPublnDateAlreadyDefinedInTargetFormat() throws Exception {
        // given
        String publnDate = "2000-05-30";

        // execute
        Patent.Builder patentBuilder = parser.parse(buildXMLContentsWithPublnDate(publnDate), getPatentBuilderInitializedWithRequiredFields());

        // assert
        Patent patent = patentBuilder.build();
        assertNotNull(patent);
        assertEquals("2000-05-30", patent.getEarliestPublnDate());
    }

    @Test
    public void testExtractFromDocumentWithPublnDateDefinedInUnsupportedFormat() throws Exception {
        // given
        String publnDate = "30/05/99";

        // execute
        Patent.Builder patentBuilder = parser.parse(buildXMLContentsWithPublnDate(publnDate), getPatentBuilderInitializedWithRequiredFields());

        // assert
        Patent patent = patentBuilder.build();
        assertNotNull(patent);
        assertEquals("30/05/99", patent.getEarliestPublnDate());
    }
    
    @Test
    public void testExtractFromDocumentWithApplicantNamesWithoutCountry() throws Exception {
        // given
        String title = null;
        String abstractText = null;
        String classIpcText = null;
        String applnEpodocId = null;
        String applnDate = null;
        String publnDate = null;
        List<String> epodocApplicantNames = Lists.newArrayList("John Smith");
        
        String xmlContents = buildXMLContents(title, abstractText, classIpcText, applnEpodocId, applnDate, publnDate,
                epodocApplicantNames);

        // execute
        Patent.Builder patentBuilder = parser.parse(xmlContents, getPatentBuilderInitializedWithRequiredFields());
        
        // assert
        Patent patent = patentBuilder.build();
        assertNotNull(patent);
        assertNotNull(patent.getApplicantNames());
        assertEquals("John Smith", patent.getApplicantNames().get(0));
        
        assertNotNull(patent.getApplicantCountryCodes());
        assertEquals(1, patent.getApplicantCountryCodes().size());
        assertEquals("", patent.getApplicantCountryCodes().get(0));
    }
    
    @Test
    public void testExtractFromDocumentWithApplicantNamesToBeCleanedUp() throws Exception {
        // given
        String title = null;
        String abstractText = null;
        String classIpcText = null;
        String applnEpodocId = null;
        String applnDate = null;
        String publnDate = null;
        List<String> epodocApplicantNames = Lists.newArrayList("Smith J., ", "    Doe J.");
        
        String xmlContents = buildXMLContents(title, abstractText, classIpcText, applnEpodocId, applnDate, publnDate,
                epodocApplicantNames);

        // execute
        Patent.Builder patentBuilder = parser.parse(xmlContents, getPatentBuilderInitializedWithRequiredFields());

        // assert
        Patent patent = patentBuilder.build();
        assertNotNull(patent);
        assertNotNull(patent.getApplicantCountryCodes());
        assertEquals("", patent.getApplicantCountryCodes().get(0));
        assertEquals("", patent.getApplicantCountryCodes().get(1));
        
        assertNotNull(patent.getApplicantNames());
        assertEquals(2, patent.getApplicantNames().size());
        assertEquals("Smith J.", patent.getApplicantNames().get(0));
        assertEquals("Doe J.", patent.getApplicantNames().get(1));
    }
    
    // -------------------------------- PRIVATE ----------------------------
    
    private Patent.Builder getPatentBuilderInitializedWithRequiredFields() {
        Patent.Builder patentBuilder = Patent.newBuilder();
        patentBuilder.setApplnAuth("irrelevant");
        patentBuilder.setApplnNr("irrelevant");
        return patentBuilder;
    }

    private String buildXMLContentsWithPublnDate(String publnDate) throws ParserConfigurationException, TransformerException {
        return buildXMLContents(null, null, null, null, null, publnDate, null);
    }
    
    private String buildXMLContents(String title, String abstractText, String classIpcText, String applnEpodocId,
            String applnDate, String publnDate, List<String> epodocApplicantNames) throws ParserConfigurationException, TransformerException {
    
        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

        Document doc = docBuilder.newDocument();
        Element rootElement = doc.createElement("world-patent-data");
        doc.appendChild(rootElement);

        Element exchangeDocs = doc.createElement("exchange-documents");
        rootElement.appendChild(exchangeDocs);

        Element exchangeDoc = doc.createElement("exchange-document");
        exchangeDocs.appendChild(exchangeDoc);
        
        if (abstractText != null) {
            Element abstractEl = doc.createElement("abstract");
            // currently the lang is unspecified
            abstractEl.appendChild(doc.createTextNode(abstractText));
            exchangeDoc.appendChild(abstractEl);
        }
        
        Element biblioData = doc.createElement("bibliographic-data");
        exchangeDoc.appendChild(biblioData);
        
        if (title != null) {
            Element titleEl = doc.createElement("invention-title");
            titleEl.appendChild(doc.createTextNode(title));
            biblioData.appendChild(titleEl);
        }
        
        if (classIpcText != null) {
            Element classIpc = doc.createElement("classification-ipc");
            biblioData.appendChild(classIpc);    
            
            Element classIpcTextEl = doc.createElement("text");
            classIpcTextEl.appendChild(doc.createTextNode(classIpcText));
            classIpc.appendChild(classIpcTextEl);
            
        }
        
        {
            Element applnReference = doc.createElement("application-reference");
            biblioData.appendChild(applnReference);
            
            Element documentId = doc.createElement("document-id");
            documentId.setAttribute("document-id-type", "epodoc");
            applnReference.appendChild(documentId);
            
            if (applnEpodocId != null) {
                Element docNumber = doc.createElement("doc-number");
                docNumber.appendChild(doc.createTextNode(applnEpodocId));
                documentId.appendChild(docNumber);
            }
            if (applnDate != null) {
                Element date = doc.createElement("date");
                date.appendChild(doc.createTextNode(applnDate));
                documentId.appendChild(date);
            }    
        }
        
        {
            Element publnReference = doc.createElement("publication-reference");
            biblioData.appendChild(publnReference);
            
            Element documentId = doc.createElement("document-id");
            documentId.setAttribute("document-id-type", "epodoc");
            publnReference.appendChild(documentId);

            if (publnDate != null) {
                Element date = doc.createElement("date");
                date.appendChild(doc.createTextNode(publnDate));
                documentId.appendChild(date);
            }            
        }

        Element parties = doc.createElement("parties");
        biblioData.appendChild(parties);
        
        Element applicants = doc.createElement("applicants");
        parties.appendChild(applicants);
        
        if (CollectionUtils.isNotEmpty(epodocApplicantNames)) {
            for (String epodocApplicantName : epodocApplicantNames) {
                Element applicant = doc.createElement("applicant");
                applicant.setAttribute("data-format", "epodoc");
                applicants.appendChild(applicant);
                
                Element applicantName = doc.createElement("applicant-name");
                applicant.appendChild(applicantName);
                
                Element name = doc.createElement("name");
                name.appendChild(doc.createTextNode(epodocApplicantName));
                applicantName.appendChild(name);
            }
        }
        
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        DOMSource source = new DOMSource(doc);
        StringWriter strWriter = new StringWriter();
        transformer.transform(source, new StreamResult(strWriter));
        return strWriter.toString();
    }

}
