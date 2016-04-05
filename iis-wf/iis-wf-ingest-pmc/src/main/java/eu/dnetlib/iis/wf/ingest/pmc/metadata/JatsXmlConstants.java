package eu.dnetlib.iis.wf.ingest.pmc.metadata;

/**
 * Constants defining element names, attribute names and attribute values from JATS xml
 * 
 * @author mhorst
 * @author madryk
 *
 */
public interface JatsXmlConstants {

//  article root element, relevant when parsing article record nested inside oai record.
    String ELEM_ARTICLE = "article";
    
//  front journal
    String ELEM_JOURNAL_TITLE = "journal-title";
    String ELEM_JOURNAL_META = "journal-meta";
//  front article
    String ELEM_ARTICLE_META = "article-meta";
    String ELEM_ARTICLE_ID = "article-id";
    String ELEM_AFFILIATION = "aff";
    String ATTR_AFFILIATION_ID = "id";
    String ELEM_LABEL = "label";
    String ELEM_SUP = "sup";
    String ELEM_COUNTRY = "country";
    String ELEM_INSTITUTION = "institution";
    
    
//  back citations
    String ELEM_REF_LIST = "ref-list";
    String ELEM_REF = "ref";
    String ELEM_PUB_ID = "pub-id";
//  back citations meta
    String ELEM_ARTICLE_TITLE = "article-title";
    String ELEM_SOURCE = "source";
    String ELEM_YEAR = "year";
    String ELEM_VOLUME = "volume";
    String ELEM_ISSUE = "issue";
    String ELEM_FPAGE = "fpage";
    String ELEM_LPAGE = "lpage";
//  back citations author
    String ELEM_NAME = "name";
    String ELEM_SURNAME = "surname";
    String ELEM_GIVEN_NAMES = "given-names";
//  back citations contains text child
    String ELEM_CITATION = "citation";
    String ELEM_ELEMENT_CITATION = "element-citation";
    String ELEM_MIXED_CITATION = "mixed-citation";
//  attributes
    String PUB_ID_TYPE = "pub-id-type";
    String ATTR_ARTICLE_TYPE = "article-type";
    
    // contributors
    String ELEM_CONTRIBUTOR_GROUP = "contrib-group";
    String ELEM_CONTRIBUTOR = "contrib";
    String ATTR_CONTRIBUTOR_TYPE = "contrib-type";
    String ATTR_VALUE_AUTHOR = "author";
    
    // reference
    String ELEM_XREF = "xref";
    String ATTR_XREF_TYPE = "ref-type";
    String ATTR_AFFILIATION_XREF = "aff";
    String ATTR_XREF_ID = "rid";
}
