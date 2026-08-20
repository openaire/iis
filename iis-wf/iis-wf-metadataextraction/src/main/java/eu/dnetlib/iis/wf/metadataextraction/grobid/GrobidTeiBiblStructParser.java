package eu.dnetlib.iis.wf.metadataextraction.grobid;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import eu.dnetlib.iis.wf.metadataextraction.parser.ParsedReference;

/**
 * Parser converting the TEI XML output of Grobid's {@code /api/processCitation}
 * endpoint (a single {@code <biblStruct>}) into {@link ParsedReference} fields.
 * <p>
 * The extraction is local-name based so it works regardless of whether the TEI
 * document declares the TEI XML namespace.
 *
 * @author mhorst
 */
public final class GrobidTeiBiblStructParser {

    private GrobidTeiBiblStructParser() {
    }

    /**
     * Parses the first {@code <biblStruct>} found in the given TEI XML.
     *
     * @param teiXml TEI XML returned by Grobid citation processing
     * @return parsed fields, or null when no {@code <biblStruct>} could be found
     */
    public static ParsedReference parse(String teiXml) throws Exception {
        Document document = parseXml(teiXml);
        Element root = document.getDocumentElement();
        // Grobid may return the biblStruct wrapped in a TEI document or as a bare root element
        Element biblStruct = null;
        if (root != null && "biblStruct".equals(nodeName(root))) {
            biblStruct = root;
        } else {
            biblStruct = firstElementByLocalName(root, "biblStruct");
        }
        if (biblStruct == null) {
            return null;
        }

        ParsedReference parsed = new ParsedReference();

        Element analytic = firstChildByLocalName(biblStruct, "analytic");
        Element monogr = firstChildByLocalName(biblStruct, "monogr");

        // title - analytic preferred, monogr fallback
        String title = firstText(firstElementByLocalName(analytic, "title"));
        if (isBlank(title)) {
            title = firstText(firstElementByLocalName(monogr, "title"));
        }
        parsed.setTitle(title);

        // authors - under analytic for journal articles, under monogr for books/theses
        List<String> authors = new ArrayList<>();
        for (Element container : new Element[]{analytic, monogr}) {
            if (container != null) {
                for (Element author : childrenByLocalName(container, "author")) {
                    String name = buildAuthorName(author);
                    if (isNotBlank(name)) {
                        authors.add(name);
                    }
                }
            }
        }
        parsed.setAuthors(authors);

        // journal / source container title
        parsed.setJournal(firstText(firstElementByLocalName(monogr, "title")));

        // imprint-based fields
        Element imprint = firstChildByLocalName(monogr, "imprint");
        if (imprint != null) {
            parsed.setVolume(biblScopeValue(imprint, "volume"));
            parsed.setIssue(biblScopeValue(imprint, "issue"));
            parsed.setPages(biblScopePages(imprint));
            parsed.setYear(dateValue(imprint));
            parsed.setLocation(firstText(firstChildByLocalName(imprint, "pubPlace")));
        }

        // publisher - monogr level preferred, imprint fallback
        String publisher = firstText(firstChildByLocalName(monogr, "publisher"));
        if (isBlank(publisher) && imprint != null) {
            publisher = firstText(firstChildByLocalName(imprint, "publisher"));
        }
        parsed.setPublisher(publisher);

        // edition
        parsed.setEdition(firstText(firstChildByLocalName(monogr, "edition")));

        // series
        Element series = firstChildByLocalName(monogr, "series");
        if (series != null) {
            parsed.setSeries(firstText(firstChildByLocalName(series, "title")));
        }

        // url
        parsed.setUrl(ptrTarget(biblStruct));

        // external identifiers
        for (Element idno : allElementsByLocalName(biblStruct, "idno")) {
            String type = idno.getAttribute("type");
            String value = idno.getTextContent().trim();
            if ("DOI".equalsIgnoreCase(type)) {
                parsed.setDoi(value);
            } else if ("ISBN".equalsIgnoreCase(type)) {
                parsed.setIsbn(value);
            } else if ("ISSN".equalsIgnoreCase(type)) {
                parsed.setIssn(value);
            }
        }

        return parsed;
    }

    // ----------------------------- PRIVATE -----------------------------

    private static Document parseXml(String xml) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(false);
        InputSource is = new InputSource(new StringReader(xml));
        return factory.newDocumentBuilder().parse(is);
    }

    private static String buildAuthorName(Element author) {
        Element persName = firstChildByLocalName(author, "persName");
        if (persName == null) {
            return author.getTextContent().trim();
        }
        StringBuilder name = new StringBuilder();
        for (Element forename : childrenByLocalName(persName, "forename")) {
            String forenameText = forename.getTextContent().trim();
            if (isNotBlank(forenameText)) {
                if (name.length() > 0) {
                    name.append(' ');
                }
                name.append(forenameText);
            }
        }
        String surname = firstText(firstElementByLocalName(persName, "surname"));
        if (isNotBlank(surname)) {
            if (name.length() > 0) {
                name.append(' ');
            }
            name.append(surname);
        }
        return name.toString().trim();
    }

    private static String biblScopeValue(Element parent, String unit) {
        for (Element biblScope : childrenByLocalName(parent, "biblScope")) {
            if (unit.equals(biblScope.getAttribute("unit"))) {
                return biblScope.getTextContent().trim();
            }
        }
        return null;
    }

    private static String biblScopePages(Element parent) {
        for (Element biblScope : childrenByLocalName(parent, "biblScope")) {
            if ("page".equals(biblScope.getAttribute("unit"))) {
                String from = biblScope.getAttribute("from");
                String to = biblScope.getAttribute("to");
                if (isNotBlank(from) && isNotBlank(to)) {
                    return from + "-" + to;
                }
                if (isNotBlank(from)) {
                    return from;
                }
                return biblScope.getTextContent().trim();
            }
        }
        return null;
    }

    private static String dateValue(Element parent) {
        Element date = firstChildByLocalName(parent, "date");
        if (date == null) {
            return null;
        }
        String raw = date.getAttribute("when");
        if (isBlank(raw)) {
            raw = date.getTextContent();
        }
        return extractYear(raw);
    }

    private static final Pattern YEAR_PREFIX_PATTERN = Pattern.compile("^(\\d{4})(?:-|$)");

    /**
     * Extracts the leading 4-digit year from a date that may be in YYYY, YYYY-MM or YYYY-MM-DD format.
     */
    private static String extractYear(String value) {
        if (isBlank(value)) {
            return null;
        }
        Matcher matcher = YEAR_PREFIX_PATTERN.matcher(value.trim());
        return matcher.find() ? matcher.group(1) : null;
    }

    private static String ptrTarget(Element biblStruct) {
        Element ptr = firstElementByLocalName(biblStruct, "ptr");
        if (ptr != null) {
            String target = ptr.getAttribute("target");
            if (isNotBlank(target)) {
                return target.trim();
            }
        }
        // fallback: idno of type URL
        for (Element idno : allElementsByLocalName(biblStruct, "idno")) {
            if ("URL".equalsIgnoreCase(idno.getAttribute("type"))) {
                return idno.getTextContent().trim();
            }
        }
        return null;
    }

    private static String firstText(Element element) {
        if (element == null) {
            return null;
        }
        String text = element.getTextContent();
        return isBlank(text) ? null : text.trim();
    }

    // ----------------------------- DOM HELPERS -----------------------------

    private static Element firstChildByLocalName(Element parent, String localName) {
        if (parent == null) {
            return null;
        }
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE && localName.equals(nodeName(node))) {
                return (Element) node;
            }
        }
        return null;
    }

    private static List<Element> childrenByLocalName(Element parent, String localName) {
        List<Element> result = new ArrayList<>();
        if (parent == null) {
            return result;
        }
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE && localName.equals(nodeName(node))) {
                result.add((Element) node);
            }
        }
        return result;
    }

    private static Element firstElementByLocalName(Element parent, String localName) {
        if (parent == null) {
            return null;
        }
        NodeList descendants = parent.getElementsByTagName("*");
        for (int i = 0; i < descendants.getLength(); i++) {
            Node node = descendants.item(i);
            if (localName.equals(nodeName(node))) {
                return (Element) node;
            }
        }
        return null;
    }

    private static List<Element> allElementsByLocalName(Element parent, String localName) {
        List<Element> result = new ArrayList<>();
        if (parent == null) {
            return result;
        }
        NodeList descendants = parent.getElementsByTagName("*");
        for (int i = 0; i < descendants.getLength(); i++) {
            Node node = descendants.item(i);
            if (localName.equals(nodeName(node))) {
                result.add((Element) node);
            }
        }
        return result;
    }

    private static String nodeName(Node node) {
        String localName = node.getLocalName();
        return localName != null ? localName : node.getNodeName();
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static boolean isNotBlank(String value) {
        return !isBlank(value);
    }
}
