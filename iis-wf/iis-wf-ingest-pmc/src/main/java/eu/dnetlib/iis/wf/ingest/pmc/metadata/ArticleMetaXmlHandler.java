package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.*;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.TagHierarchyUtils.*;

import java.util.List;
import java.util.Stack;

import org.apache.commons.lang.StringUtils;
import org.jdom.Element;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.importer.CermineAffiliation;
import eu.dnetlib.iis.common.importer.CermineAffiliationBuilder;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.Affiliation;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.Author;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.Range;
import pl.edu.icm.cermine.exception.AnalysisException;
import pl.edu.icm.cermine.exception.TransformationException;
import pl.edu.icm.cermine.metadata.affiliation.CRFAffiliationParser;

/**
 * Sax xml handler of &lt;article-meta&gt; tag in JATS xml
 * 
 * @author madryk
 */
public class ArticleMetaXmlHandler extends DefaultHandler implements ProcessingFinishedAwareXmlHandler {
    
    private Stack<String> parents;
    
    private final ExtractedDocumentMetadata.Builder builder;
    
    private String currentValue;
    
    private CermineAffiliationBuilder cermineAffiliationBuilder = new CermineAffiliationBuilder();
    private CermineToIngestAffConverter cermineToIngestAffConverter = new CermineToIngestAffConverter();
    
    private String currentArticleIdType = null;
    
    private StringBuilder affiliationText = new StringBuilder();
    private String currentAffiliationId = null;
    
    private JatsAuthor currentAuthor;
    private List<JatsAuthor> currentAuthorsGroup = Lists.newArrayList();
    private List<JatsAuthor> currentAuthors = Lists.newArrayList();
    
    
    //------------------------ CONSTRUCTOS --------------------------
    
    public ArticleMetaXmlHandler(ExtractedDocumentMetadata.Builder builder) {
        this.builder = builder;
        this.parents = new Stack<String>();
    }
    
    
    //------------------------ LOGIC --------------------------
    
    @Override
    public void startDocument() throws SAXException {
        this.parents = new Stack<String>();
        builder.setAffiliations(Lists.newArrayList());
        builder.setAuthors(Lists.newArrayList());
    }
    
    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        
        
        if (isElement(qName, ELEM_AFFILIATION)) {
            affiliationText.setLength(0);
            currentAffiliationId = attributes.getValue(ATTR_AFFILIATION_ID);
        } else if (isElement(qName, ELEM_ARTICLE_ID)) {
            currentArticleIdType = attributes.getValue(PUB_ID_TYPE);
        } else if (isElement(qName, ELEM_CONTRIBUTOR)) {
            if (ATTR_VALUE_AUTHOR.equals(attributes.getValue(ATTR_CONTRIBUTOR_TYPE))) {
                currentAuthor = new JatsAuthor();
            }
        } else if (isWithinElement(qName, ELEM_XREF, parents, ELEM_CONTRIBUTOR)) {
            if (currentAuthor != null && ATTR_AFFILIATION_XREF.equals(attributes.getValue(ATTR_XREF_TYPE))) {
                String affId = attributes.getValue(ATTR_XREF_ID);
                if (affId != null) {
                    currentAuthor.getAffiliationRefId().add(affId);
                }
            }
        }
        
        
        this.parents.push(qName);
    }
    
    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        
        this.currentValue = new String(ch, start, length);
        
        if (hasAmongParents(parents, ELEM_AFFILIATION)) {
            
            // skipping affiliation position element
            if (!hasAmongParents(parents, ELEM_LABEL) && !hasAmongParents(parents, ELEM_SUP)) {
                this.affiliationText.append(new String(ch, start, length));
            }
            
        }
        
    }
    
    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        this.parents.pop();
        
        if (isElement(qName, ELEM_ARTICLE_ID) && currentArticleIdType != null) {
            builder.getExternalIdentifiers().put(currentArticleIdType, currentValue.trim());
        } else if (isElement(qName, ELEM_FPAGE)) {
            if (builder.getPages()==null) {
                builder.setPages(Range.newBuilder().build());
            }
            builder.getPages().setStart(this.currentValue.trim());
        } else if (isElement(qName, ELEM_LPAGE)) {
            if (builder.getPages()==null) {
                builder.setPages(Range.newBuilder().build());
            }
            builder.getPages().setEnd(this.currentValue.trim());
            
        } else if (isElement(qName, ELEM_AFFILIATION)) {
            handleAffiliation();
        } else if (isElement(qName, ELEM_CONTRIBUTOR) || hasAmongParents(parents, ELEM_CONTRIBUTOR)) {
            if (currentAuthor != null) { // currently handling a contributor which is an author
                
                if (isWithinElement(qName, ELEM_SURNAME, parents, ELEM_NAME)) {
                    currentAuthor.setSurname(currentValue.trim());
                } else if (isWithinElement(qName, ELEM_GIVEN_NAMES, parents, ELEM_NAME)) {
                    currentAuthor.setGivenNames(currentValue.trim());
                } else if (isElement(qName, ELEM_CONTRIBUTOR)) {
                    currentAuthorsGroup.add(currentAuthor);
                    currentAuthor = null;
                }
                
            }
        } else if (isElement(qName, ELEM_CONTRIBUTOR_GROUP)) {
            currentAuthors.addAll(currentAuthorsGroup);
            currentAuthorsGroup.clear();
        }
        
    }
    
    @Override
    public void endDocument() throws SAXException {
        for (JatsAuthor pmcAuthor : currentAuthors) {
            Author author = Author.newBuilder()
                    .setFullname(pmcAuthor.getSurname() + ", " + pmcAuthor.getGivenNames())
                    .setAffiliationPositions(pmcAuthor.getAffiliationPos())
                    .build();
            builder.getAuthors().add(author);
        }
    }

    @Override
    public boolean hasFinished() {
        return parents.isEmpty();
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void handleAffiliation() throws SAXException {
        
        Affiliation currentAffiliation = buildAffiliation();
        if (currentAffiliation != null) {
            int currentAffiliationPosition = builder.getAffiliations().size();
            
            if (assignAuthorsForAffiliation(currentAffiliation, currentAffiliationPosition)) {
                builder.getAffiliations().add(currentAffiliation);
            }
            
        }
    }
    
    private Affiliation buildAffiliation() throws SAXException {
        
        try {
            String affStr = this.affiliationText.toString();
            if (StringUtils.isNotBlank(affStr)) {
                CRFAffiliationParser affiliationParser = new CRFAffiliationParser();
                Element parsedAffiliation = affiliationParser.parse(affStr);
                if (parsedAffiliation!=null) {
                    CermineAffiliation cAff = cermineAffiliationBuilder.build(parsedAffiliation);
                    Affiliation aff = cermineToIngestAffConverter.convert(cAff);
                    return aff;
                }
            }
        } catch (AnalysisException e) {
            throw new SAXException("unexpected exception while parsing "
                    + "affiliations for document: " + builder.getId(), e);
        } catch (TransformationException e) {
            throw new SAXException("unexpected exception while parsing "
                    + "affiliations for document: " + builder.getId(), e);
        }
        
        return null;
    }
    
    private boolean assignAuthorsForAffiliation(Affiliation currentAffiliation, int currentAffiliationPosition) throws SAXException {
        boolean atLeastOneAuthorAssigned = false;
        
        if (hasAmongParents(parents, ELEM_CONTRIBUTOR)) {
            currentAuthor.getAffiliationPos().add(currentAffiliationPosition);
            atLeastOneAuthorAssigned = true;
        } else if (hasAmongParents(parents, ELEM_CONTRIBUTOR_GROUP)) {
            for (JatsAuthor author : currentAuthorsGroup) {
                author.getAffiliationPos().add(currentAffiliationPosition);
                atLeastOneAuthorAssigned = true;
            }
        } else if (currentAffiliationId != null) {
            for (JatsAuthor author : currentAuthors) {
                for (String affRefId : author.getAffiliationRefId()) {
                    if (StringUtils.equals(currentAffiliationId, affRefId)) {
                        author.getAffiliationPos().add(currentAffiliationPosition);
                        atLeastOneAuthorAssigned = true;
                    }
                }
            }
        } else {
            throw new SAXException("unable to connect affiliation with corresponding authors (" + currentAffiliation + ")");
        }
        
        return atLeastOneAuthorAssigned;
    }
}