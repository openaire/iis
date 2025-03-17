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
 * Transformer responsible for converting TEI XML documents (produced by Grobid) 
 * into ExtractedDocumentMetadata Avro records.
 */
public class TeiToExtractedDocumentMetadataTransformer {
    private static final Logger logger = LoggerFactory.getLogger(TeiToExtractedDocumentMetadataTransformer.class);

    private static final String TEI_NAMESPACE = "http://www.tei-c.org/ns/1.0";
    
    /**
     * Transforms TEI XML content into an ExtractedDocumentMetadata record.
     * 
     * @param id document identifier
     * @param teiXml TEI XML content
     * @return ExtractedDocumentMetadata record
     * @throws Exception if transformation fails
     */
    public ExtractedDocumentMetadata transformToExtractedDocumentMetadata(String id, String teiXml) throws Exception {
        Document document = parseXmlString(teiXml);
        XPath xPath = XPathFactory.newInstance().newXPath();
        
        // Extracting the full text content
        String fullText = extractTextContent(document, xPath);
        
        // Create ExtractedDocumentMetadata with id and text
        ExtractedDocumentMetadata.Builder documentBuilder = ExtractedDocumentMetadata.newBuilder()
                .setId(id)
                .setText(fullText);
        
        // Extract and set title
        String title = extractSingleValue(document, xPath, "//tei:titleStmt/tei:title");
        if (StringUtils.isNotEmpty(title)) {
            documentBuilder.setTitle(title);
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
        String journal = extractSingleValue(document, xPath, "//tei:seriesStmt/tei:title");
        if (StringUtils.isNotEmpty(journal)) {
            documentBuilder.setJournal(journal);
        }
        
        // Extract volume, issue, and pages
        String volume = extractSingleValue(document, xPath, "//tei:biblStruct//tei:biblScope[@unit='volume']");
        if (StringUtils.isNotEmpty(volume)) {
            documentBuilder.setVolume(volume);
        }
        
        String issue = extractSingleValue(document, xPath, "//tei:biblStruct//tei:biblScope[@unit='issue']");
        if (StringUtils.isNotEmpty(issue)) {
            documentBuilder.setIssue(issue);
        }
        
        Range pageRange = extractPageRange(document, xPath);
        if (pageRange != null) {
            documentBuilder.setPages(pageRange);
        }
        
        // Extract publisher information
        String publisher = extractSingleValue(document, xPath, "//tei:publicationStmt/tei:publisher");
        if (StringUtils.isNotEmpty(publisher)) {
            documentBuilder.setPublisher(publisher);
        }
        
        // Extract keywords
        List<CharSequence> keywords = extractKeywords(document, xPath);
        if (keywords != null && !keywords.isEmpty()) {
            documentBuilder.setKeywords(keywords);
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
        
        // Extract language if available
        String language = extractLanguage(document, xPath);
        if (StringUtils.isNotEmpty(language)) {
            documentBuilder.setLanguage(language);
        }
        
        // Extract publication type
        String pubType = extractPublicationType(document, xPath);
        if (StringUtils.isNotEmpty(pubType)) {
            documentBuilder.setPublicationTypeName(pubType);
        }
        
        return documentBuilder.build();
    }

    private Document parseXmlString(String xml) throws ParserConfigurationException, SAXException, IOException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        DocumentBuilder builder = factory.newDocumentBuilder();
        InputSource is = new InputSource(new StringReader(xml));
        return builder.parse(is);
    }

    private String extractTextContent(Document document, XPath xPath) throws XPathExpressionException {
        // FIXME this method should be reimplemented according to earlier experiments conducted in:
        // https://git.icm.edu.pl/mhorst/grobid-integration-experiments
        StringBuilder textBuilder = new StringBuilder();
        
        // Extract text from all div elements
        NodeList divs = (NodeList) xPath.evaluate("//tei:div", document, XPathConstants.NODESET);
        for (int i = 0; i < divs.getLength(); i++) {
            Node div = divs.item(i);
            textBuilder.append(div.getTextContent().trim()).append("\n\n");
        }
        
        return textBuilder.toString().trim();
    }

    // FIXME this was autogenerated, fixed by me, but it's not perfect and might need further adjustments
    private String extractSingleValue(Node node, XPath xPath, String expression) {
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

    private String extractAbstract(Document document, XPath xPath) {
        try {
            // Look for abstract in different possible locations
            Node abstractNode = (Node) xPath.evaluate("//tei:profileDesc/tei:abstract", document, XPathConstants.NODE);
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

    private Integer extractPublicationYear(Document document, XPath xPath) {
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

    private Range extractPageRange(Document document, XPath xPath) {
        try {
            Node pageNode = (Node) xPath.evaluate("//tei:biblStruct//tei:biblScope[@unit='page']", document, XPathConstants.NODE);
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

    private List<CharSequence> extractKeywords(Document document, XPath xPath) {
        try {
            NodeList keywordNodes = (NodeList) xPath.evaluate("//tei:keywords/tei:term", document, XPathConstants.NODESET);
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

    private void extractAuthorsAndAffiliations(Document document, XPath xPath, 
                                              List<Author> authors, List<Affiliation> affiliations) {
        try {
            // Extract affiliations first
            NodeList affiliationNodes = (NodeList) xPath.evaluate("//tei:sourceDesc//tei:affiliation", document, XPathConstants.NODESET);
            Map<String, Integer> affiliationIndexMap = new HashMap<>();
            
            for (int i = 0; i < affiliationNodes.getLength(); i++) {
                Element affNode = (Element) affiliationNodes.item(i);
                String affId = affNode.getAttribute("key");
                
                Affiliation.Builder affBuilder = Affiliation.newBuilder();
                
                String orgName = extractSingleValue(affNode, xPath, ".//tei:orgName");
                if (StringUtils.isNotEmpty(orgName)) {
                    affBuilder.setOrganization(orgName);
                }
                
                String country = extractSingleValue(affNode, xPath, ".//tei:country");
                if (StringUtils.isNotEmpty(country)) {
                    affBuilder.setCountryName(country);
                }
                
                String address = extractSingleValue(affNode, xPath, ".//tei:address");
                if (StringUtils.isNotEmpty(address)) {
                    affBuilder.setAddress(address);
                }
                
                Affiliation affiliation = affBuilder.build();
                affiliations.add(affiliation);
                
                // Store the index position
                if (StringUtils.isNotEmpty(affId)) {
                    affiliationIndexMap.put(affId, i);
                }
            }
            
            // Extract authors
            NodeList authorNodes = (NodeList) xPath.evaluate("//tei:sourceDesc//tei:author", document, XPathConstants.NODESET);
            
            for (int i = 0; i < authorNodes.getLength(); i++) {
                Element authorNode = (Element) authorNodes.item(i);
                
                // Extract author name
                String authorName = extractSingleValue(authorNode, xPath, ".//tei:persName");
                if (StringUtils.isEmpty(authorName)) {
                    // Try alternative structures
                    String forename = extractSingleValue(authorNode, xPath, ".//tei:forename");
                    String surname = extractSingleValue(authorNode, xPath, ".//tei:surname");
                    
                    if (StringUtils.isNotEmpty(forename) || StringUtils.isNotEmpty(surname)) {
                        authorName = (StringUtils.defaultString(forename) + " " + StringUtils.defaultString(surname)).trim();
                    }
                }
                
                if (StringUtils.isNotEmpty(authorName)) {
                    Author.Builder authorBuilder = Author.newBuilder().setAuthorFullName(authorName);
                    
                    // Find author's affiliations
                    NodeList authorAffRefs = (NodeList) xPath.evaluate(".//tei:affiliation", authorNode, XPathConstants.NODESET);
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

    private List<ReferenceMetadata> extractReferences(Document document, XPath xPath) {
        List<ReferenceMetadata> references = new ArrayList<>();
        try {
            NodeList referenceNodes = (NodeList) xPath.evaluate("//tei:text//tei:ref[@type='bibr']", document, XPathConstants.NODESET);
            
            for (int i = 0; i < referenceNodes.getLength(); i++) {
                Element refNode = (Element) referenceNodes.item(i);
                String refId = refNode.getAttribute("target");
                
                if (StringUtils.isEmpty(refId)) {
                    continue;
                }
                
                // Clean reference ID (removing # if present)
                if (refId.startsWith("#")) {
                    refId = refId.substring(1);
                }
                
                // Try to find the reference content
                String query = String.format("//tei:text//tei:bibl[@xml:id='%s']", refId);
                Node biblNode = (Node) xPath.evaluate(query, document, XPathConstants.NODE);
                
                if (biblNode == null) {
                    // Try alternative query formats
                    biblNode = (Node) xPath.evaluate("//tei:text//tei:biblStruct[@xml:id='" + refId + "']", document, XPathConstants.NODE);
                }
                
                if (biblNode != null) {
                    ReferenceMetadata.Builder refBuilder = ReferenceMetadata.newBuilder();
                    ReferenceBasicMetadata.Builder basicMetadataBuilder = ReferenceBasicMetadata.newBuilder();
                    
                    // Extract reference position
                    try {
                        String positionText = refNode.getTextContent();
                        // Extract number from content like "[1]" or "1"
                        Pattern posPattern = Pattern.compile("\\[?(\\d+)\\]?");
                        Matcher posMatcher = posPattern.matcher(positionText);
                        if (posMatcher.find()) {
                            int position = Integer.parseInt(posMatcher.group(1));
                            refBuilder.setPosition(position);
                        }
                    } catch (Exception e) {
                        logger.debug("Could not extract position for reference: " + refId, e);
                    }
                    
                    // Extract reference text
                    refBuilder.setText(biblNode.getTextContent().trim());
                    
                    // Extract title
                    String title = extractSingleValue(biblNode, xPath, ".//tei:title");
                    if (StringUtils.isNotEmpty(title)) {
                        basicMetadataBuilder.setTitle(title);
                    }
                    
                    // Extract authors
                    NodeList refAuthorNodes = (NodeList) xPath.evaluate(".//tei:author", biblNode, XPathConstants.NODESET);
                    if (refAuthorNodes != null && refAuthorNodes.getLength() > 0) {
                        List<CharSequence> refAuthors = new ArrayList<>();
                        for (int j = 0; j < refAuthorNodes.getLength(); j++) {
                            String authorName = refAuthorNodes.item(j).getTextContent().trim();
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
                    if (StringUtils.isNotEmpty(yearStr)) {
                        try {
                            Pattern yearPattern = Pattern.compile("\\b(19|20)\\d{2}\\b");
                            Matcher yearMatcher = yearPattern.matcher(yearStr);
                            if (yearMatcher.find()) {
                                basicMetadataBuilder.setYear(yearMatcher.group());
                            }
                        } catch (NumberFormatException e) {
                            logger.debug("Could not parse year from: " + yearStr);
                        }
                    }
                    
                    // Extract source (journal/book title)
                    String source = extractSingleValue(biblNode, xPath, ".//tei:monogr/tei:title");
                    if (StringUtils.isNotEmpty(source)) {
                        basicMetadataBuilder.setSource(source);
                    }
                    
                    // Set basic metadata on reference
                    refBuilder.setBasicMetadata(basicMetadataBuilder.build());
                    
                    references.add(refBuilder.build());
                }
            }
        } catch (XPathExpressionException e) {
            logger.warn("Error extracting references", e);
        }
        return references;
    }

    private String extractLanguage(Document document, XPath xPath) {
        try {
            Node langNode = (Node) xPath.evaluate("//tei:profileDesc/tei:langUsage/tei:language", document, XPathConstants.NODE);
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

    private String extractPublicationType(Document document, XPath xPath) {
        try {
            Node typeNode = (Node) xPath.evaluate("//tei:profileDesc/tei:textClass/tei:keywords/tei:term[@type='publication_type']", 
                                                document, XPathConstants.NODE);
            if (typeNode != null) {
                return typeNode.getTextContent().trim();
            }
            
            // Try to determine the type based on other clues
            boolean hasJournal = StringUtils.isNotEmpty(extractSingleValue(document, xPath, "//tei:seriesStmt/tei:title"));
            boolean hasAbstract = StringUtils.isNotEmpty(extractAbstract(document, xPath));
            
            if (hasJournal && hasAbstract) {
                return "journal article";
            }
        } catch (XPathExpressionException e) {
            logger.warn("Error extracting publication type", e);
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
            return null;
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
            return Collections.singletonList(getPrefix(namespaceURI)).iterator();
        }
    }
}
