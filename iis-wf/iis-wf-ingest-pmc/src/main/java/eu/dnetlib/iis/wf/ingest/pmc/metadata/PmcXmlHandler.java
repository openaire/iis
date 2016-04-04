package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import static eu.dnetlib.iis.wf.ingest.pmc.metadata.PmcXmlConstants.*;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.TagHierarchyUtils.*;

import java.util.HashMap;
import java.util.Map;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.collect.Maps;

import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata;


/**
 * PMC XML SAX handler.
 * 
 * @author mhorst
 *
 */
public class PmcXmlHandler extends DefaultHandler {

    private boolean rootElement = true;

    private final ExtractedDocumentMetadata.Builder builder;
    
    private XmlSwitcherHandler xmlSwitcherHandler;

    /**
     * Default constructor.
     * @param receiver
     */
    public PmcXmlHandler(ExtractedDocumentMetadata.Builder builder) {
        super();
        this.builder = builder;
        if (!this.builder.hasExternalIdentifiers()) {
            this.builder.setExternalIdentifiers(new HashMap<CharSequence, CharSequence>());
        }
        
        Map<String, ParentAwareXmlHandler> handlers = Maps.newHashMap();
        handlers.put(ELEM_JOURNAL_META, new JournalMetaXmlHandler(builder));
        handlers.put(ELEM_ARTICLE_META, new ArticleMetaXmlHandler(builder));
        handlers.put(ELEM_REF_LIST, new RefListXmlHandler(builder));
        xmlSwitcherHandler = new XmlSwitcherHandler(builder, handlers);
    }

    @Override
    public void startDocument() throws SAXException {
        clearAllFields();
        xmlSwitcherHandler.startDocument();
    }

    @Override
    public void startElement(String uri, String localName, String qName,
            Attributes attributes) throws SAXException {

        if (rootElement || isElement(qName, ELEM_ARTICLE)) {
            rootElement = false;
            // extracting article-type from root element or from article element nested in oai record
            String articleType = attributes.getValue(ATTR_ARTICLE_TYPE);
            if (articleType!=null) {
                builder.setEntityType(articleType);	
            } else {
                builder.setEntityType("unknown");
            }
        }
        
        xmlSwitcherHandler.startElement(uri, localName, qName, attributes);

    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        xmlSwitcherHandler.endElement(uri, localName, qName);
    }

    @Override
    public void endDocument() throws SAXException {
        xmlSwitcherHandler.endDocument();
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        xmlSwitcherHandler.characters(ch, start, length);
    }

    private void clearAllFields() {
        this.rootElement = true;
    }



}
