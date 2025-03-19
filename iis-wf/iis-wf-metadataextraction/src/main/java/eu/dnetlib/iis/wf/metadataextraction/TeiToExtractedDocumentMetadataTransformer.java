package eu.dnetlib.iis.wf.metadataextraction;

import eu.dnetlib.iis.metadataextraction.schemas.Affiliation;
import eu.dnetlib.iis.metadataextraction.schemas.Author;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.Range;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Transformer responsible for converting TEI XML documents (produced by Grobid) into ExtractedDocumentMetadata Avro records.
 * @author mhorst
 */
public class TeiToExtractedDocumentMetadataTransformer {
    
    private static final Logger logger = LoggerFactory.getLogger(TeiToExtractedDocumentMetadataTransformer.class);

    private static final String TEI_NAMESPACE = "http://www.tei-c.org/ns/1.0";
    
    // ------------------------------------------ LOGIC ---------------------------------------------------

    /**
     * Transforms TEI XML content into an ExtractedDocumentMetadata record.
     * 
     * @param id     document identifier
     * @param teiXml TEI XML content
     * @return ExtractedDocumentMetadata record
     * @throws Exception if transformation fails
     */
    public static ExtractedDocumentMetadata transformToExtractedDocumentMetadata(String id, String teiXml) throws Exception {
        if (id == null) {
            throw new RuntimeException("unable to set null id");
        }
        Document document = parseXmlString(teiXml);
        XPath xPath = XPathFactory.newInstance().newXPath();

        // Set the namespace context for XPath
        xPath.setNamespaceContext(new TeiNamespaceContext());

        // Extracting the full text content
        String fullText = extractTextContent(id, teiXml);

        // Create ExtractedDocumentMetadata with id and text
        ExtractedDocumentMetadata.Builder documentBuilder = ExtractedDocumentMetadata.newBuilder().setId(id)
                .setText(fullText);

        // Extract and set title, prioritizing main title
        String mainTitle = extractSingleValue(document, xPath, "//tei:teiHeader//tei:titleStmt/tei:title[@type='main']");
        if (StringUtils.isNotEmpty(mainTitle)) {
            documentBuilder.setTitle(mainTitle);
        } else {
            String title = extractSingleValue(document, xPath, "//tei:teiHeader//tei:titleStmt/tei:title");    
            if (StringUtils.isNotEmpty(title)) {
                documentBuilder.setTitle(title);
            }   
        }

        // Extract and set abstract
        String abstractText = extractAbstract(document, xPath);
        if (StringUtils.isNotEmpty(abstractText)) {
            documentBuilder.setAbstract$(abstractText);
        }

        // Extract publication year
        Integer year = extractPublicationYear(document, xPath);
        if (year != null) {
            documentBuilder.setYear(year);
        }

        // Extract journal information
        String mainJournalTitle = extractSingleValue(document, xPath, "//tei:teiHeader//tei:monogr/tei:title[@type='main']");
        if (StringUtils.isNotEmpty(mainJournalTitle)) {
            documentBuilder.setJournal(mainJournalTitle);
        } else {
            String journalTitle = extractSingleValue(document, xPath, "//tei:teiHeader//tei:monogr/tei:title");
            if (StringUtils.isNotEmpty(journalTitle)) {
                documentBuilder.setJournal(journalTitle);
            }    
        }

        // Extract volume, issue, and pages
        String volume = extractSingleValue(document, xPath, "//tei:teiHeader//tei:biblStruct//tei:biblScope[@unit='volume']");
        if (StringUtils.isNotEmpty(volume)) {
            documentBuilder.setVolume(volume);
        }

        String issue = extractSingleValue(document, xPath, "//tei:teiHeader//tei:biblStruct//tei:biblScope[@unit='issue']");
        if (StringUtils.isNotEmpty(issue)) {
            documentBuilder.setIssue(issue);
        }

        Range pageRange = extractPageRange(document, xPath);
        if (pageRange != null) {
            documentBuilder.setPages(pageRange);
        }

        // Extract publisher information
        String publisher = extractSingleValue(document, xPath, "//tei:teiHeader//tei:publicationStmt/tei:publisher");
        if (StringUtils.isNotEmpty(publisher)) {
            documentBuilder.setPublisher(publisher);
        }

        // Extract keywords
        List<CharSequence> keywords = extractKeywords(document, xPath);
        if (keywords != null && !keywords.isEmpty()) {
            documentBuilder.setKeywords(keywords);
        }
        
        Map<CharSequence, CharSequence> externalIdentifiers = extractExternalIdentifiers(document, xPath);
        if (externalIdentifiers != null && !externalIdentifiers.isEmpty()) {
            documentBuilder.setExternalIdentifiers(externalIdentifiers);
        }
        
        // Extract language if available
        String language = extractLanguage(document, xPath);
        if (StringUtils.isNotEmpty(language)) {
            documentBuilder.setLanguage(language);
        }

        // Extract authors and affiliations
        List<Author> authors = new ArrayList<>();
        List<Affiliation> affiliations = new ArrayList<>();
        extractAuthorsAndAffiliations(document, xPath, authors, affiliations);

        if (!authors.isEmpty()) {
            documentBuilder.setAuthors(authors);
        }

        if (!affiliations.isEmpty()) {
            documentBuilder.setAffiliations(affiliations);
        }

        // Extract references
        List<ReferenceMetadata> references = extractReferences(document, xPath);
        if (!references.isEmpty()) {
            documentBuilder.setReferences(references);
        }

        return documentBuilder.build();
    }
    
    // ------------------------------------------ PRIVATE ---------------------------------------------------

    private static Document parseXmlString(String xml) throws ParserConfigurationException, SAXException, IOException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        DocumentBuilder builder = factory.newDocumentBuilder();
        InputSource is = new InputSource(new StringReader(xml));
        return builder.parse(is);
    }

    /**
     * Returns plaintext representation of the document.
     * This method was taken from:
     * https://git.icm.edu.pl/mhorst/grobid-integration-experiments/-/blob/master/src/main/java/com/example/GrobidProcessor.java?ref_type=heads
     * Align no the way of extracting fields (XPath used in other methods)
     * @param id
     * @param teiXml
     */
    private static String extractTextContent(String id, String teiXml) throws Exception {
        StringBuilder plainText = new StringBuilder();

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(new InputSource(new StringReader(teiXml)));

            // Extract title
            NodeList titleNodes = document.getElementsByTagName("title");
            if (titleNodes.getLength() > 0) {
                plainText.append(titleNodes.item(0).getTextContent().trim()).append("\n\n");
            }

            // Extract abstract
            NodeList abstractNodes = document.getElementsByTagName("abstract");
            if (abstractNodes.getLength() > 0) {
                plainText.append("ABSTRACT\n").append(abstractNodes.item(0).getTextContent().trim()).append("\n\n");
            }

            // Extract body text
            NodeList divNodes = document.getElementsByTagName("div");
            for (int i = 0; i < divNodes.getLength(); i++) {
                Element divElement = (Element) divNodes.item(i);

                // Get heading if available
                NodeList headNodes = divElement.getElementsByTagName("head");
                if (headNodes.getLength() > 0) {
                    plainText.append(headNodes.item(0).getTextContent().trim()).append("\n\n");
                }

                // Get paragraphs
                NodeList paragraphs = divElement.getElementsByTagName("p");
                for (int j = 0; j < paragraphs.getLength(); j++) {
                    plainText.append(paragraphs.item(j).getTextContent().trim()).append("\n\n");
                }
            }

            // Extract references/bibliography
            NodeList bibNodes = document.getElementsByTagName("listBibl");
            if (bibNodes.getLength() > 0) {
                plainText.append("REFERENCES\n");
                NodeList bibEntries = ((Element) bibNodes.item(0)).getElementsByTagName("biblStruct");
                for (int i = 0; i < bibEntries.getLength(); i++) {
                    plainText.append(i + 1).append(". ")
                            .append(bibEntries.item(i).getTextContent().trim().replaceAll("\\s+", " ")).append("\n");
                }
            }

        } catch (ParserConfigurationException | SAXException | IOException e) {
            logger.error("Error extracting plaintext from TEI XML: {}", e.getMessage());
            throw new Exception("Error extracting text from the document: " + id, e);
        }

        return plainText.toString();
    }
    
    private static String extractSingleValue(Node node, XPath xPath, String expression) {
        try {
            Node resultNode = (Node) xPath.evaluate(expression, node, XPathConstants.NODE);
            if (resultNode != null) {
                return resultNode.getTextContent().trim();
            }
        } catch (XPathExpressionException e) {
            logger.warn("Error extracting value with XPath: " + expression, e);
        }
        return null;
    }

    private static Node extractSingleNode(Node node, XPath xPath, String expression) {
        try {
            Node resultNode = (Node) xPath.evaluate(expression, node, XPathConstants.NODE);
            if (resultNode != null) {
                return resultNode;
            }
        } catch (XPathExpressionException e) {
            logger.warn("Error extracting value with XPath: " + expression, e);
        }
        return null;
    }
    
    private static String extractSingleConcatValueFromChildNodes(Node node, XPath xPath, String expression) {
        try {
            StringBuilder concatenatedNameBuilder = new StringBuilder(); 
            NodeList nodes = (NodeList) xPath.evaluate(expression, node, XPathConstants.NODESET);
            if (nodes != null && nodes.getLength() > 0) {
                for (int j = 0; j < nodes.getLength(); j++) {
                    String currentTextContent = nodes.item(j).getTextContent().trim();
                    if (StringUtils.isNotEmpty(currentTextContent)) {
                        concatenatedNameBuilder.append(currentTextContent);
                        if (j < nodes.getLength()-1) {
                            concatenatedNameBuilder.append(", ");
                        }
                    }
                }
                if (StringUtils.isNotEmpty(concatenatedNameBuilder)) {
                    return concatenatedNameBuilder.toString().trim();
                }
            }
        } catch (XPathExpressionException e) {
            logger.warn("Error extracting value with XPath: " + expression, e);
        }
        return null;
    }
    
    private static String extractAbstract(Document document, XPath xPath) {
        try {
            // Look for abstract in different possible locations
            Node abstractNode = (Node) xPath.evaluate("//tei:teiHeader//tei:profileDesc/tei:abstract", document, XPathConstants.NODE);
            if (abstractNode == null) {
                // Try alternative location
                abstractNode = (Node) xPath.evaluate("//tei:div[@type='abstract']", document, XPathConstants.NODE);
            }

            if (abstractNode != null) {
                return abstractNode.getTextContent().trim();
            }

            // If no dedicated abstract section is found, use the first paragraph as abstract
            Node firstParagraph = (Node) xPath.evaluate("(//tei:div/tei:p)[1]", document, XPathConstants.NODE);
            if (firstParagraph != null) {
                String text = firstParagraph.getTextContent().trim();
                // Limit abstract length if it's taken from first paragraph
                if (text.length() > 500) {
                    return text.substring(0, 497) + "...";
                }
                return text;
            }
        } catch (XPathExpressionException e) {
            logger.warn("Error extracting abstract", e);
        }
        return null;
    }

    private static Integer extractPublicationYear(Document document, XPath xPath) {
        String yearStr = extractSingleValue(document, xPath, "//tei:publicationStmt/tei:date/@when");
        if (StringUtils.isEmpty(yearStr)) {
            yearStr = extractSingleValue(document, xPath, "//tei:biblStruct//tei:date/@when");
        }

        if (StringUtils.isNotEmpty(yearStr)) {
            try {
                // Extract year from ISO date format (YYYY-MM-DD)
                if (yearStr.length() >= 4) {
                    return Integer.parseInt(yearStr.substring(0, 4));
                }

                // Try to parse as a standalone year
                return Integer.parseInt(yearStr);
            } catch (NumberFormatException e) {
                logger.warn("Error parsing year: " + yearStr, e);
            }
        }
        return null;
    }

    private static Range extractPageRange(Document document, XPath xPath) {
        try {
            Node pageNode = (Node) xPath.evaluate("//tei:teiHeader//tei:biblStruct//tei:biblScope[@unit='page']", document,
                    XPathConstants.NODE);
            if (pageNode != null) {
                Element pageElem = (Element) pageNode;
                String from = pageElem.getAttribute("from");
                String to = pageElem.getAttribute("to");

                if (StringUtils.isEmpty(from) && StringUtils.isEmpty(to)) {
                    String content = pageElem.getTextContent().trim();
                    String[] parts = content.split("-");
                    if (parts.length == 2) {
                        from = parts[0].trim();
                        to = parts[1].trim();
                    } else if (parts.length == 1) {
                        from = parts[0].trim();
                        to = from;
                    }
                }

                if (StringUtils.isNotEmpty(from)) {
                    Range.Builder rangeBuilder = Range.newBuilder();
                    rangeBuilder.setStart(from);
                    if (StringUtils.isNotEmpty(to)) {
                        rangeBuilder.setEnd(to);
                    } else {
                        rangeBuilder.setEnd(from);
                    }
                    return rangeBuilder.build();
                }
            }
        } catch (XPathExpressionException e) {
            logger.warn("Error extracting page range", e);
        }
        return null;
    }
    
    private static List<CharSequence> extractKeywords(Document document, XPath xPath) {
        try {
            NodeList keywordNodes = (NodeList) xPath.evaluate("//tei:teiHeader//tei:keywords/tei:term", document,
                    XPathConstants.NODESET);
            if (keywordNodes != null && keywordNodes.getLength() > 0) {
                List<CharSequence> keywords = new ArrayList<>();
                for (int i = 0; i < keywordNodes.getLength(); i++) {
                    String keyword = keywordNodes.item(i).getTextContent().trim();
                    if (StringUtils.isNotEmpty(keyword)) {
                        keywords.add(keyword);
                    }
                }
                return keywords;
            }
        } catch (XPathExpressionException e) {
            logger.warn("Error extracting keywords", e);
        }
        return null;
    }

    private static Map<CharSequence, CharSequence> extractExternalIdentifiers(Document document, XPath xPath) {
        try {
            // FIXME make sure we cover all the cases
            NodeList extIdNodes = (NodeList) xPath.evaluate("//tei:teiHeader//tei:biblStruct/tei:idno", document,
                    XPathConstants.NODESET);
            if (extIdNodes != null && extIdNodes.getLength() > 0) {
                Map<CharSequence, CharSequence> externalIds = new HashMap<>();
                for (int i = 0; i < extIdNodes.getLength(); i++) {
                    Element currentIdElement = (Element) extIdNodes.item(i);
                    String extIdKey = currentIdElement.getAttribute("type");
                    String extIdValue = currentIdElement.getTextContent().trim();
                    if (StringUtils.isNotEmpty(extIdKey) && StringUtils.isNotEmpty(extIdValue)) {
                        externalIds.put(extIdKey, extIdValue);
                    }
                }
                return externalIds;
            }
        } catch (XPathExpressionException e) {
            logger.warn("Error extracting external identifiers", e);
        }
        return null;
    }

    private static void extractAuthorsAndAffiliations(Document document, XPath xPath, List<Author> authors,
            List<Affiliation> affiliations) {
        try {
            // Extract affiliations first
            NodeList affiliationNodes = (NodeList) xPath.evaluate("//tei:teiHeader//tei:sourceDesc//tei:affiliation", document,
                    XPathConstants.NODESET);
            Map<String, Integer> affiliationIndexMap = new HashMap<>();

            for (int i = 0; i < affiliationNodes.getLength(); i++) {
                Element affNode = (Element) affiliationNodes.item(i);
                String affId = affNode.getAttribute("key");

                Affiliation.Builder affBuilder = Affiliation.newBuilder();
                
                // Extract raw text of the affiliation (required field)
                Node rawAffNode = extractSingleNode(affNode, xPath, ".//tei:note[@type='raw_affiliation']");
                // getting last child in order to skip unwanted label element whenever defined
                String rawText = rawAffNode.getLastChild().getTextContent().trim();
                if (StringUtils.isNotEmpty(rawText)) {
                    affBuilder.setRawText(rawText);    
                } else {
                    // rawText is a mandatory field
                    // FIXME should we build an affiliation on our own when missing? 
                    throw new RuntimeException("no raw affiliation text defined for affiliation!");
                }
                
                // building an organization name from multiple orgName elements
                // relying on original elements order without relying on type attribute
                affBuilder.setOrganization(extractSingleConcatValueFromChildNodes(affNode, xPath, ".//tei:orgName"));
                
                Node countryNode = extractSingleNode(affNode, xPath, ".//tei:country");
                if (countryNode != null) {
                    // setting country name
                    String country = countryNode.getTextContent().trim();
                    if (StringUtils.isNotEmpty(country)) {
                        affBuilder.setCountryName(country);
                    }
                    // setting country node
                    // FIXME if country code unspecified then rely on https://github.com/openaire/iis/issues/1510 solution or CERMINE
                    String countryCode = ((Element)countryNode).getAttribute("key");
                    if (StringUtils.isNotEmpty(countryCode)) {
                        affBuilder.setCountryCode(countryCode);
                    }
                }

                // building an address from multiple address subelements
                // relying on original elements order without relying on type attribute
                // FIXME make sure there are always child elements (such as settlement, region, country) 
                // or if it could potentially include text content alone 
                affBuilder.setAddress(extractSingleConcatValueFromChildNodes(affNode, xPath, ".//tei:address/*"));

                Affiliation affiliation = affBuilder.build();
                affiliations.add(affiliation);

                // Store the index position
                if (StringUtils.isNotEmpty(affId)) {
                    affiliationIndexMap.put(affId, i);
                }
            }

            // Extract authors
            NodeList authorNodes = (NodeList) xPath.evaluate("//tei:sourceDesc//tei:author", document,
                    XPathConstants.NODESET);

            for (int i = 0; i < authorNodes.getLength(); i++) {
                Element authorNode = (Element) authorNodes.item(i);
                
                StringBuffer authorName = new StringBuffer();
                NodeList forenames = authorNode.getElementsByTagName("forename");
                for(int j=0; j<forenames.getLength(); j++) {
                    Node forename = forenames.item(j);
                    authorName.append(forename.getTextContent());
                    authorName.append(' ');
                }
                NodeList surnames = authorNode.getElementsByTagName("surname");
                for(int j=0; j<surnames.getLength(); j++) {
                    Node surname = surnames.item(j);
                    authorName.append(surname.getTextContent());
                    authorName.append(' ');
                }

                if (StringUtils.isNotEmpty(authorName)) {
                    Author.Builder authorBuilder = Author.newBuilder().setAuthorFullName(authorName.toString().trim());

                    // Find author's affiliations
                    NodeList authorAffRefs = (NodeList) xPath.evaluate(".//tei:affiliation", authorNode,
                            XPathConstants.NODESET);
                    if (authorAffRefs != null && authorAffRefs.getLength() > 0) {
                        List<Integer> affPositions = new ArrayList<>();

                        for (int j = 0; j < authorAffRefs.getLength(); j++) {
                            Element affRef = (Element) authorAffRefs.item(j);
                            String affKey = affRef.getAttribute("key");

                            if (StringUtils.isNotEmpty(affKey) && affiliationIndexMap.containsKey(affKey)) {
                                affPositions.add(affiliationIndexMap.get(affKey));
                            }
                        }

                        if (!affPositions.isEmpty()) {
                            authorBuilder.setAffiliationPositions(affPositions);
                        }
                    }

                    authors.add(authorBuilder.build());
                }
            }

        } catch (XPathExpressionException e) {
            logger.warn("Error extracting authors and affiliations", e);
        }
    }

    private static List<ReferenceMetadata> extractReferences(Document document, XPath xPath) {
        List<ReferenceMetadata> references = new ArrayList<>();
        try {
            // Look for references in biblStruct elements within the bibliography section
            NodeList biblStructNodes = (NodeList) xPath.evaluate("//tei:listBibl/tei:biblStruct | //tei:back//tei:biblStruct | //tei:div[@type='references']//tei:biblStruct", document, XPathConstants.NODESET);
            
            // If we didn't find any references through the common patterns, try direct access to biblStruct elements
            if (biblStructNodes == null || biblStructNodes.getLength() == 0) {
                biblStructNodes = (NodeList) xPath.evaluate("//tei:biblStruct", document, XPathConstants.NODESET);
            }
            
            // Alternative approach looking for bibl elements which may also contain references
            if (biblStructNodes == null || biblStructNodes.getLength() == 0) {
                biblStructNodes = (NodeList) xPath.evaluate("//tei:bibl", document, XPathConstants.NODESET);
            }
            
            for (int i = 0; biblStructNodes != null && i < biblStructNodes.getLength(); i++) {
                Element biblNode = (Element) biblStructNodes.item(i);
                String refId = biblNode.getAttribute("xml:id");
                
                // Create reference metadata
                ReferenceMetadata.Builder refBuilder = ReferenceMetadata.newBuilder();
                ReferenceBasicMetadata.Builder basicMetadataBuilder = ReferenceBasicMetadata.newBuilder();
                
                // Set position based on index if not available otherwise
                refBuilder.setPosition(i + 1);
                
                // Look for a corresponding reference in the main text
                if (StringUtils.isNotEmpty(refId)) {
                    try {
                        NodeList refNodes = (NodeList) xPath.evaluate("//tei:ref[@target='#" + refId + "']", document, XPathConstants.NODESET);
                        if (refNodes != null && refNodes.getLength() > 0) {
                            Element refNode = (Element) refNodes.item(0);
                            String positionText = refNode.getTextContent().trim();
                            Pattern posPattern = Pattern.compile("\\[?(\\d+)\\]?");
                            Matcher posMatcher = posPattern.matcher(positionText);
                            if (posMatcher.find()) {
                                refBuilder.setPosition(Integer.parseInt(posMatcher.group(1)));
                            }
                        }
                    } catch (Exception e) {
                        logger.debug("Could not extract position for reference: " + refId, e);
                    }
                }
                
                // Extract reference text - ensures we have at least some text
                String rawText = extractSingleValue(biblNode, xPath, ".//tei:note[@type='raw_reference']");
                if (StringUtils.isNotEmpty(rawText)) {
                    refBuilder.setText(rawText);    
                } else {
                    // FIXME should we build raw text on our own when missing?
                    // this is autogenerated code but it does not seem to be ok to return unnormalized text including tons of white characters
                    // refBuilder.setText(biblNode.getTextContent().trim()); 
                }
                
                // Extract title
                String title = extractSingleValue(biblNode, xPath, ".//tei:title");
                if (StringUtils.isEmpty(title)) {
                    // Try alternative title locations
                    title = extractSingleValue(biblNode, xPath, ".//tei:analytic//tei:title");
                    if (StringUtils.isEmpty(title)) {
                        title = extractSingleValue(biblNode, xPath, ".//tei:monogr//tei:title");
                    }
                }
                
                if (StringUtils.isNotEmpty(title)) {
                    basicMetadataBuilder.setTitle(title);
                }
                
                // Extract authors
                NodeList refAuthorNodes = (NodeList) xPath.evaluate(".//tei:author", biblNode, XPathConstants.NODESET);
                if (refAuthorNodes == null || refAuthorNodes.getLength() == 0) {
                    // Try alternative author locations
                    refAuthorNodes = (NodeList) xPath.evaluate(".//tei:analytic//tei:author", biblNode, XPathConstants.NODESET);
                }
                
                if (refAuthorNodes != null && refAuthorNodes.getLength() > 0) {
                    List<CharSequence> refAuthors = new ArrayList<>();
                    for (int j = 0; j < refAuthorNodes.getLength(); j++) {
                        Element authorElem = (Element) refAuthorNodes.item(j);
                        String authorName;
                        
                        // Try to extract structured name parts
                        String forename = extractSingleValue(authorElem, xPath, ".//tei:forename");
                        String surname = extractSingleValue(authorElem, xPath, ".//tei:surname");
                        
                        if (StringUtils.isNotEmpty(forename) || StringUtils.isNotEmpty(surname)) {
                            authorName = (StringUtils.defaultString(forename) + " " + StringUtils.defaultString(surname)).trim();
                        } else {
                            authorName = authorElem.getTextContent().trim();
                        }
                        
                        if (StringUtils.isNotEmpty(authorName)) {
                            refAuthors.add(authorName);
                        }
                    }
                    
                    if (!refAuthors.isEmpty()) {
                        basicMetadataBuilder.setAuthors(refAuthors);
                    }
                }
                
                // Extract year
                String yearStr = extractSingleValue(biblNode, xPath, ".//tei:date");
                if (StringUtils.isEmpty(yearStr)) {
                    // Try to find date with when attribute
                    yearStr = extractSingleValue(biblNode, xPath, ".//tei:date/@when");
                }
                
                if (StringUtils.isNotEmpty(yearStr)) {
                    try {
                        // First try to extract year from attribute or ISO format
                        if (yearStr.length() >= 4 && yearStr.matches("\\d{4}.*")) {
                            basicMetadataBuilder.setYear(yearStr.substring(0, 4));
                        } else {
                            // Try to find year pattern in text
                            Pattern yearPattern = Pattern.compile("\\b(19|20)\\d{2}\\b");
                            Matcher yearMatcher = yearPattern.matcher(yearStr);
                            if (yearMatcher.find()) {
                                basicMetadataBuilder.setYear(yearMatcher.group());
                            }
                        }
                    } catch (Exception e) {
                        logger.debug("Could not parse year from: " + yearStr, e);
                    }
                }
                
                // Extract source (journal/book title)
                String source = extractSingleValue(biblNode, xPath, ".//tei:monogr/tei:title");
                if (StringUtils.isEmpty(source)) {
                    // Try alternative source locations
                    source = extractSingleValue(biblNode, xPath, ".//tei:imprint/tei:publisher");
                }
                
                if (StringUtils.isNotEmpty(source)) {
                    basicMetadataBuilder.setSource(source);
                }
                
                // Set basic metadata on reference
                refBuilder.setBasicMetadata(basicMetadataBuilder.build());
                references.add(refBuilder.build());
            }
            
            // If we still don't have any references, try to extract from any references in text
            if (references.isEmpty()) {
                NodeList refNodes = (NodeList) xPath.evaluate("//tei:ref[@type='bibr']", document, XPathConstants.NODESET);
                for (int i = 0; refNodes != null && i < refNodes.getLength(); i++) {
                    Element refNode = (Element) refNodes.item(i);
                    String refText = refNode.getTextContent().trim();
                    
                    if (StringUtils.isNotEmpty(refText)) {
                        ReferenceMetadata.Builder refBuilder = ReferenceMetadata.newBuilder();
                        refBuilder.setText(refText);
                        
                        // Try to extract position from text like [1] or (1)
                        Pattern posPattern = Pattern.compile("[\\[\\(]?(\\d+)[\\]\\)]?");
                        Matcher posMatcher = posPattern.matcher(refText);
                        if (posMatcher.find()) {
                            refBuilder.setPosition(Integer.parseInt(posMatcher.group(1)));
                        } else {
                            refBuilder.setPosition(i + 1);
                        }
                        
                        references.add(refBuilder.build());
                    }
                }
            }
            
        } catch (XPathExpressionException e) {
            logger.warn("Error extracting references", e);
        }
        
        return references;
    }

    private static String extractLanguage(Document document, XPath xPath) {
        try {
            Node langNode = (Node) xPath.evaluate("//tei:teiHeader//tei:profileDesc/tei:langUsage/tei:language", document,
                    XPathConstants.NODE);
            if (langNode != null) {
                String langCode = ((Element) langNode).getAttribute("ident");
                if (StringUtils.isNotEmpty(langCode)) {
                    return langCode;
                }
                return langNode.getTextContent().trim();
            }
        } catch (XPathExpressionException e) {
            logger.warn("Error extracting language", e);
        }
        return null;
    }

    /**
     * A simple namespace context implementation to handle TEI namespace.
     */
    private static class TeiNamespaceContext implements javax.xml.namespace.NamespaceContext {
        @Override
        public String getNamespaceURI(String prefix) {
            if ("tei".equals(prefix)) {
                return TEI_NAMESPACE;
            }
            return javax.xml.XMLConstants.NULL_NS_URI;
        }

        @Override
        public String getPrefix(String namespaceURI) {
            if (TEI_NAMESPACE.equals(namespaceURI)) {
                return "tei";
            }
            return null;
        }

        @Override
        public Iterator<String> getPrefixes(String namespaceURI) {
            if (TEI_NAMESPACE.equals(namespaceURI)) {
                return Collections.singletonList("tei").iterator();
            }
            return Collections.emptyIterator();
        }
    }
}
