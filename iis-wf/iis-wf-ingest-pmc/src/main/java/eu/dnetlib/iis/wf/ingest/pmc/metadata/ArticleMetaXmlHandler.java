package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.ATTR_AFFILIATION_ID;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.ATTR_AFFILIATION_XREF;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.ATTR_CONTRIBUTOR_TYPE;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.ATTR_VALUE_AUTHOR;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.ATTR_XREF_ID;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.ATTR_XREF_TYPE;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.ELEM_AFFILIATION;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.ELEM_ARTICLE_ID;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.ELEM_CONTRIBUTOR;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.ELEM_CONTRIBUTOR_GROUP;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.ELEM_FPAGE;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.ELEM_GIVEN_NAMES;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.ELEM_LABEL;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.ELEM_LPAGE;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.ELEM_NAME;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.ELEM_SUP;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.ELEM_SURNAME;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.ELEM_XREF;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlConstants.PUB_ID_TYPE;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.TagHierarchyUtils.hasAmongParents;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.TagHierarchyUtils.isElement;
import static eu.dnetlib.iis.wf.ingest.pmc.metadata.TagHierarchyUtils.isWithinElement;

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
    
    /**
     * Maximum affiliation lenght required due to Mallet library limitation causing StackOverflowError
     * https://github.com/openaire/iis/issues/663 
     */
    private static final int MAX_AFF_LENGTH = 3000;
    
    private Stack<String> parents;
    
    private final ExtractedDocumentMetadata.Builder builder;
    
    private String currentValue;
    
    private final CermineAffiliationBuilder cermineAffiliationBuilder = new CermineAffiliationBuilder();
    private final CermineToIngestAffConverter cermineToIngestAffConverter = new CermineToIngestAffConverter();
    
    private String currentArticleIdType;
    
    private final StringBuilder affiliationText = new StringBuilder();
    private String currentAffiliationId;
    
    private final StringBuilder authorText = new StringBuilder();
    private JatsAuthor currentAuthor;
    private final List<JatsAuthor> currentAuthorsGroup = Lists.newArrayList();
    private final List<JatsAuthor> currentAuthors = Lists.newArrayList();
    
    
    //------------------------ CONSTRUCTOS --------------------------
    
    public ArticleMetaXmlHandler(ExtractedDocumentMetadata.Builder builder) {
        super();
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
                this.affiliationText.append(currentValue);
            }
            
        } else if (currentAuthor != null && hasAmongParents(parents, ELEM_NAME)) {
            this.authorText.append(currentValue);
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
        } else if (isElement(qName, ELEM_CONTRIBUTOR_GROUP)) {
            currentAuthors.addAll(currentAuthorsGroup);
            currentAuthorsGroup.clear();
        } else if (isElement(qName, ELEM_CONTRIBUTOR) || hasAmongParents(parents, ELEM_CONTRIBUTOR)) {
            if (currentAuthor != null) { // currently handling a contributor which is an author
                
                if (isWithinElement(qName, ELEM_SURNAME, parents, ELEM_NAME)) {
                    currentAuthor.setSurname(authorText.toString().trim());
                    authorText.setLength(0);
                } else if (isWithinElement(qName, ELEM_GIVEN_NAMES, parents, ELEM_NAME)) {
                    currentAuthor.setGivenNames(authorText.toString().trim());
                    authorText.setLength(0);
                } else if (isElement(qName, ELEM_CONTRIBUTOR)) {
                    if (StringUtils.isNotBlank(currentAuthor.getSurname()) || StringUtils.isNotBlank(currentAuthor.getGivenNames())) {
                        currentAuthorsGroup.add(currentAuthor);
                    }
                    authorText.setLength(0);
                    currentAuthor = null;
                }
            }
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
            builder.getAffiliations().add(currentAffiliation);
            assignAuthorsForAffiliation(currentAffiliationPosition);
        }
        
        affiliationText.setLength(0);
    }
    
    private Affiliation buildAffiliation() throws SAXException {
        
        try {
            String affStr = this.affiliationText.toString();
            if (StringUtils.isNotBlank(affStr) && affStr.length() <= MAX_AFF_LENGTH) {
                CRFAffiliationParser affiliationParser = new CRFAffiliationParser();
                Element parsedAffiliation = affiliationParser.parse(affStr);
                if (parsedAffiliation!=null) {
                    CermineAffiliation cAff = cermineAffiliationBuilder.build(parsedAffiliation);
                    return cermineToIngestAffConverter.convert(cAff);
                }
            }
        } catch (TransformationException | AnalysisException e) {
            throw new SAXException("unexpected exception while parsing "
                    + "affiliations for document: " + builder.getId(), e);
        }
        
        return null;
    }
    
    private void assignAuthorsForAffiliation(int currentAffiliationPosition) throws SAXException {
        if (currentAffiliationId != null) {
            for (JatsAuthor author : currentAuthors) {
                for (String affRefId : author.getAffiliationRefId()) {
                    if (StringUtils.equals(currentAffiliationId, affRefId)) {
                        author.getAffiliationPos().add(currentAffiliationPosition);
                    }
                }
            }
        } else if (hasAmongParents(parents, ELEM_CONTRIBUTOR)) {
            if (currentAuthor != null) {
                currentAuthor.getAffiliationPos().add(currentAffiliationPosition);    
            }
        } else if (hasAmongParents(parents, ELEM_CONTRIBUTOR_GROUP)) {
            for (JatsAuthor author : currentAuthorsGroup) {
                author.getAffiliationPos().add(currentAffiliationPosition);
            }
        }
    }
}