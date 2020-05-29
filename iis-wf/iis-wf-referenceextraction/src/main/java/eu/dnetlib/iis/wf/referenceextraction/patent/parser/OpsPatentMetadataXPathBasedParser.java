package eu.dnetlib.iis.wf.referenceextraction.patent.parser;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;

/**
 * Pull parser processing XML records obtained from OPS EPO public endpoint.
 * 
 * @author mhorst
 *
 */
public class OpsPatentMetadataXPathBasedParser implements PatentMetadataParser {

    /**
     * 
     */
    private static final long serialVersionUID = 2157218881928411205L;
    
    private static final Logger log = Logger.getLogger(OpsPatentMetadataXPathBasedParser.class);

    private static final String XPATH_EXPR_INVENTION_TITLE = "//bibliographic-data/invention-title";

    private static final String XPATH_EXPR_ABSTRACT = "//abstract";
    
    private static final String XPATH_EXPR_CLASS_IPC = "//bibliographic-data/classification-ipc/text";
    
    private static final String XPATH_EXPR_TEMPLATE_APLN_REFERENCE_DOC_ID = "//bibliographic-data/application-reference/document-id[@document-id-type=\"{0}\"]/doc-number";
    
    private static final String DOCUMENT_ID_TYPE_EPODOC = "epodoc";
    
    private static final String XPATH_EXPR_APLN_DATE = "//bibliographic-data/application-reference/document-id/date";
    
    private static final String XPATH_EXPR_PUBL_DATE = "//bibliographic-data/publication-reference/document-id/date";
    
    private static final String XPATH_EXPR_APPLICANT_NAME = "//bibliographic-data/parties/applicants/applicant[@data-format=\"{0}\"]";
    
    private static final String DATA_FORMAT_EPODOC = "epodoc";
    
    private static final String DATA_FORMAT_ORIGINAL = "original";
    
    private static final String ATTRIB_NAME_LANG = "lang";

    private static final String ATTRIB_VALUE_LANG_EN = "en";
    
    private static final String DATE_FORMAT_PATTERN_SOURCE = "yyyyMMdd";
    
    private static final DateFormat DATE_FORMAT_SOURCE = new SimpleDateFormat(DATE_FORMAT_PATTERN_SOURCE);
    
    private static final String DATE_FORMAT_PATTERN_TARGET = "yyyy-MM-dd";
    
    private static final DateFormat DATE_FORMAT_TARGET = new SimpleDateFormat(DATE_FORMAT_PATTERN_TARGET);
    
    private String xPathExpApplnDocIdEpodoc;
    
    private String xPathExpApplicantOriginalName;
    
    private String xPathExpApplicantEpodocName;

    private DocumentBuilderFactory builderFactory;

    private XPath xPath;
    

    // ------------------------------- CONSTRUCTOR -------------------------------
    
    public OpsPatentMetadataXPathBasedParser() {
        instantiateParser();
    }
    
    // ------------------------------- PUBLIC ------------------------------------

    @Override
    public Patent.Builder parse(CharSequence source, Patent.Builder patentBuilder) throws PatentMetadataParserException {
        try {
            DocumentBuilder builder = builderFactory.newDocumentBuilder();
            Document xmlDocument = builder.parse(new InputSource(new StringReader(source.toString())));

            patentBuilder.setApplnTitle(extractTrimmedValueForPreferedLanguage(
                    (NodeList) xPath.compile(XPATH_EXPR_INVENTION_TITLE).evaluate(xmlDocument, XPathConstants.NODESET),
                    ATTRIB_VALUE_LANG_EN));

            patentBuilder.setApplnAbstract(extractTrimmedValueForPreferedLanguage(
                    (NodeList) xPath.compile(XPATH_EXPR_ABSTRACT).evaluate(xmlDocument, XPathConstants.NODESET),
                    ATTRIB_VALUE_LANG_EN));
            
            CharSequence epoDoc = extractFirstNonEmptyTrimmedTextContent(
                    (NodeList) xPath.compile(xPathExpApplnDocIdEpodoc).evaluate(xmlDocument, XPathConstants.NODESET));
            if (StringUtils.isNotBlank(epoDoc)) {
                patentBuilder.setApplnNrEpodoc(epoDoc);    
            }

            patentBuilder.setApplnFilingDate(convertDate(extractEarliestDate(
                    (NodeList) xPath.compile(XPATH_EXPR_APLN_DATE).evaluate(xmlDocument, XPathConstants.NODESET))));
            
            patentBuilder.setEarliestPublnDate(convertDate(extractEarliestDate(
                    (NodeList) xPath.compile(XPATH_EXPR_PUBL_DATE).evaluate(xmlDocument, XPathConstants.NODESET))));
            
            List<CharSequence> ipcClasses = extractNonEmptyTrimmedTextContent(
                    (NodeList) xPath.compile(XPATH_EXPR_CLASS_IPC).evaluate(xmlDocument, XPathConstants.NODESET));
            if (CollectionUtils.isNotEmpty(ipcClasses)) {
                patentBuilder.setIpcClassSymbol(ipcClasses);
            }
            
            List<CharSequence> applicantOriginalNames = extractNonEmptyTrimmedTextContent(
                    (NodeList) xPath.compile(xPathExpApplicantOriginalName).evaluate(xmlDocument, XPathConstants.NODESET));
            if (CollectionUtils.isNotEmpty(applicantOriginalNames)) {
                patentBuilder.setApplicantNames(cleanNames(applicantOriginalNames));
            }
            
            List<CharSequence> applicantEpodocNames = extractNonEmptyTrimmedTextContent(
                    (NodeList) xPath.compile(xPathExpApplicantEpodocName).evaluate(xmlDocument, XPathConstants.NODESET));
            if (CollectionUtils.isNotEmpty(applicantEpodocNames)) {
                patentBuilder.setApplicantCountryCodes(extractCountryCodes(applicantEpodocNames));
            }
            
            return patentBuilder;

        } catch (SAXException | IOException | XPathExpressionException | ParserConfigurationException e) {
            throw new PatentMetadataParserException("error while parsing XML contents: " + source, e);
        }
    }

    // ------------------------------- PRIVATE ------------------------------------

    
    private static String extractTrimmedValueForPreferedLanguage(NodeList nodes, String preferedLang) {
        String otherTitle = null;
        for (int i = 0; i < nodes.getLength(); i++) {
            Node currentNode = nodes.item(i);
            Node langNode = currentNode.getAttributes().getNamedItem(ATTRIB_NAME_LANG);
            if (langNode != null && preferedLang.equals(langNode.getTextContent())) {
                return currentNode.getTextContent().trim();
            } else {
                otherTitle = currentNode.getTextContent().trim();
            }
        }
        return otherTitle;
    }
    
    private static List<CharSequence> extractNonEmptyTrimmedTextContent(NodeList nodes) {
        List<CharSequence> results = Lists.newArrayList();
        for (int i = 0; i < nodes.getLength(); i++) {
            Node currentNode = nodes.item(i);
            String textContent = currentNode.getTextContent();
            if (StringUtils.isNotBlank(textContent)) {
                results.add(textContent.trim());
            }
        }
        return results;
    }
    
    private static CharSequence extractFirstNonEmptyTrimmedTextContent(NodeList nodes) {
        for (int i = 0; i < nodes.getLength(); i++) {
            Node currentNode = nodes.item(i);
            String textContent = currentNode.getTextContent();
            if (StringUtils.isNotBlank(textContent)) {
                return textContent.trim();
            }
        }
        return null;
    }
    
    /**
     * Extracts earliest date comparing date strings lexicographically.
     */
    private static String extractEarliestDate(NodeList nodes) {
        String earliest = null;
        for (int i = 0; i < nodes.getLength(); i++) {
            Node currentNode = nodes.item(i);
            String textContent = currentNode.getTextContent();
            if (StringUtils.isNotBlank(textContent)) {
                String trimmedText = textContent.trim();
                if (earliest==null || trimmedText.compareTo(earliest) < 0) {
                    earliest = trimmedText;
                }
            }
        }
        return earliest;
    }
    
    /**
     * Converts date whenever specified in expected format, propagates uncoverted date otherwise.
     */
    private static String convertDate(String source) {
        if (StringUtils.isNotBlank(source)) {
            try {
                Date parsedSource = DATE_FORMAT_SOURCE.parse(source);
                if (source.equals(DATE_FORMAT_SOURCE.format(parsedSource))) {
                    return DATE_FORMAT_TARGET.format(parsedSource);
                } else {
                    return source;
                }
            } catch (ParseException e) {
                log.warn("propagating source date without conversion: source date '" + source
                        + "' is not defined in expected format: " + DATE_FORMAT_PATTERN_SOURCE);
                return source;
            }
        } else {
            return null;
        }
    }
    
    private static List<CharSequence> cleanNames(List<CharSequence> names) {
        if (CollectionUtils.isNotEmpty(names)) {
            return names.stream().map(OpsPatentMetadataXPathBasedParser::cleanName)
                    .collect(Collectors.toList());
        } else {
            return names;
        }
    }
    
    private static CharSequence cleanName(CharSequence name) {
        if (StringUtils.isNotBlank(name)) {
            if (name.charAt(name.length()-1) == ',') {
                return name.subSequence(0, name.length()-1);
            }
        } 
        return name;
        
    }
    
    private static List<CharSequence> extractCountryCodes(List<CharSequence> epodocNames) {
        if (CollectionUtils.isNotEmpty(epodocNames)) {
            return epodocNames.stream().map(OpsPatentMetadataXPathBasedParser::extractCountryCode)
                    .collect(Collectors.toList());
        } else {
            return null;
        }
    }
    
    private static CharSequence extractCountryCode(CharSequence epodocName) {
        if (StringUtils.isNotBlank(epodocName)) {
            String[] countryCodeCandidates = StringUtils.substringsBetween(epodocName.toString(), "[", "]");
            if (ArrayUtils.isNotEmpty(countryCodeCandidates)) {
                return countryCodeCandidates[countryCodeCandidates.length -1]; 
            }
        }
        return null;
    }
    
    private void instantiateParser() {
        this.xPathExpApplnDocIdEpodoc = MessageFormat.format(XPATH_EXPR_TEMPLATE_APLN_REFERENCE_DOC_ID, DOCUMENT_ID_TYPE_EPODOC);
        this.xPathExpApplicantOriginalName = MessageFormat.format(XPATH_EXPR_APPLICANT_NAME, DATA_FORMAT_ORIGINAL);
        this.xPathExpApplicantEpodocName = MessageFormat.format(XPATH_EXPR_APPLICANT_NAME, DATA_FORMAT_EPODOC);
        this.builderFactory = DocumentBuilderFactory.newInstance();
        this.xPath = XPathFactory.newInstance().newXPath();
    }
    
    /**
     * This method is part of deserialization mechanism.
     */
    private void readObject(ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        inputStream.defaultReadObject();
        instantiateParser();
    }

}
