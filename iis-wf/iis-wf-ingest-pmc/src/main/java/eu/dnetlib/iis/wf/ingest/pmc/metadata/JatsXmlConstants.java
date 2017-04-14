package eu.dnetlib.iis.wf.ingest.pmc.metadata;

/**
 * Constants defining element names, attribute names and attribute values from JATS xml
 * 
 * @author mhorst
 * @author madryk
 *
 */
public final class JatsXmlConstants {

//  article root element, relevant when parsing article record nested inside oai record.
    protected static final String ELEM_ARTICLE = "article";
    
//  front journal
    protected static final String ELEM_JOURNAL_TITLE = "journal-title";
    protected static final String ELEM_JOURNAL_META = "journal-meta";
//  front article
    protected static final String ELEM_ARTICLE_META = "article-meta";
    protected static final String ELEM_ARTICLE_ID = "article-id";
    protected static final String ELEM_AFFILIATION = "aff";
    protected static final String ATTR_AFFILIATION_ID = "id";
    protected static final String ELEM_LABEL = "label";
    protected static final String ELEM_SUP = "sup";
    protected static final String ELEM_COUNTRY = "country";
    protected static final String ELEM_INSTITUTION = "institution";
    
    
//  back citations
    protected static final String ELEM_REF_LIST = "ref-list";
    protected static final String ELEM_REF = "ref";
    protected static final String ELEM_PUB_ID = "pub-id";
//  back citations meta
    protected static final String ELEM_ARTICLE_TITLE = "article-title";
    protected static final String ELEM_SOURCE = "source";
    protected static final String ELEM_YEAR = "year";
    protected static final String ELEM_VOLUME = "volume";
    protected static final String ELEM_ISSUE = "issue";
    protected static final String ELEM_FPAGE = "fpage";
    protected static final String ELEM_LPAGE = "lpage";
//  back citations author
    protected static final String ELEM_NAME = "name";
    protected static final String ELEM_SURNAME = "surname";
    protected static final String ELEM_GIVEN_NAMES = "given-names";
//  back citations contains text child
    protected static final String ELEM_CITATION = "citation";
    protected static final String ELEM_ELEMENT_CITATION = "element-citation";
    protected static final String ELEM_MIXED_CITATION = "mixed-citation";
//  attributes
    protected static final String PUB_ID_TYPE = "pub-id-type";
    protected static final String ATTR_ARTICLE_TYPE = "article-type";
    
    // contributors
    protected static final String ELEM_CONTRIBUTOR_GROUP = "contrib-group";
    protected static final String ELEM_CONTRIBUTOR = "contrib";
    protected static final String ATTR_CONTRIBUTOR_TYPE = "contrib-type";
    protected static final String ATTR_VALUE_AUTHOR = "author";
    
    // reference
    protected static final String ELEM_XREF = "xref";
    protected static final String ATTR_XREF_TYPE = "ref-type";
    protected static final String ATTR_AFFILIATION_XREF = "aff";
    protected static final String ATTR_XREF_ID = "rid";
    
    private JatsXmlConstants() {}
    
}
