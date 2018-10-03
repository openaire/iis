package eu.dnetlib.iis.common;

import java.io.UnsupportedEncodingException;

import eu.dnetlib.data.proto.TypeProtos.Type;

/**
 * InfoSpaceConstants constants.
 * 
 * @author mhorst
 *
 */
public final class InfoSpaceConstants {

    public static final float CONFIDENCE_TO_TRUST_LEVEL_FACTOR = 0.9f;

    public static final String ENCODING_UTF8 = "utf-8";

    public static final char ROW_PREFIX_SEPARATOR = '|';

    public static final String ID_NAMESPACE_SEPARATOR = "::";
    public static final String CLASSIFICATION_HIERARCHY_SEPARATOR = ID_NAMESPACE_SEPARATOR;
    public static final String INFERENCE_PROVENANCE_SEPARATOR = ID_NAMESPACE_SEPARATOR;

    public static final String ROW_PREFIX_RESULT = "50|";
    public static final String ROW_PREFIX_PROJECT = "40|";
    public static final String ROW_PREFIX_PERSON = "30|";
    public static final String ROW_PREFIX_ORGANIZATION = "20|";
    public static final String ROW_PREFIX_DATASOURCE = "10|";

    public static final String QUALIFIER_BODY_STRING = "body";
    public static final byte[] QUALIFIER_BODY;

    public static final String SEMANTIC_CLASS_MAIN_TITLE = "main title";
    public static final String SEMANTIC_CLASS_ALTERNATIVE_TITLE = "alternative title";
    
    public static final String SEMANTIC_CLASS_PUBLICATION = "publication";
    public static final String SEMANTIC_CLASS_SOFTWARE = "software";
    public static final String SEMANTIC_CLASS_PUBLICATION_RESOURCE_SOFTWARE = "Software";
    public static final String SEMANTIC_CLASS_UNKNOWN = "UNKNOWN";
    public static final String SEMANTIC_CLASS_OPEN_SOURCE = "OPEN SOURCE";
    public static final String SEMANTIC_CLASS_IIS = "iis";
    
    public static final String SEMANTIC_CLASS_NAME_NOT_AVAILABLE = "not available";
    public static final String SEMANTIC_CLASS_NAME_OPEN_SOURCE = "Open Source";

    public static final String SEMANTIC_SCHEME_DNET_PERSON_ROLES = "dnet:personroles";
    public static final String SEMANTIC_SCHEME_DNET_RELATIONS_RESULT_RESULT = "dnet:result_result_relations";
    public static final String SEMANTIC_SCHEME_DNET_RELATIONS_RESULT_PROJECT = "dnet:result_project_relations";

    public static final String SEMANTIC_SCHEME_DNET_TITLE = "dnet:dataCite_title";
    public static final String SEMANTIC_SCHEME_DNET_TITLE_TYPOLOGIES = "dnet:title_typologies";
    public static final String SEMANTIC_SCHEME_DNET_RESULT_TYPOLOGIES = "dnet:result_typologies";
    public static final String SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS = "dnet:provenanceActions";
    public static final String SEMANTIC_SCHEME_DNET_LANGUAGES = "dnet:languages";
    public static final String SEMANTIC_SCHEME_DNET_PID_TYPES = "dnet:pid_types";
    public static final String SEMANTIC_SCHEME_DNET_CLASSIFICATION_TAXONOMIES = "dnet:subject_classification_typologies";
    public static final String SEMANTIC_SCHEME_DNET_PUBLICATION_RESOURCE = "dnet:publication_resource";
    public static final String SEMANTIC_SCHEME_DNET_ACCESS_MODES = "dnet:access_modes";

    // resultResult citation and similarity related
    public static final String SEMANTIC_SCHEME_DNET_DATASET_PUBLICATION_RELS = "dnet:dataset_publication_rels";

    public static final String SEMANTIC_CLASS_TAXONOMIES_ARXIV = "arxiv";
    public static final String SEMANTIC_CLASS_TAXONOMIES_WOS = "wos";
    public static final String SEMANTIC_CLASS_TAXONOMIES_DDC = "ddc";
    public static final String SEMANTIC_CLASS_TAXONOMIES_MESHEUROPMC = "mesheuropmc";
    public static final String SEMANTIC_CLASS_TAXONOMIES_ACM = "acm";

    public static final String EXTERNAL_ID_TYPE_INSTANCE_URL = "dnet:instance-url";
    public static final String EXTERNAL_ID_TYPE_UNKNOWN = "unknown";

    // publication resource types class ids
    public static final String SEMANTIC_CLASS_INSTANCE_TYPE_ARTICLE = "0001";
    public static final String SEMANTIC_CLASS_INSTANCE_TYPE_DATASET = "0021";
    public static final String SEMANTIC_CLASS_INSTANCE_TYPE_SOFTWARE = "0029";

    static {
        try {
            QUALIFIER_BODY = QUALIFIER_BODY_STRING.getBytes(ENCODING_UTF8);

        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns column family name for given entity type.
     * 
     * @param entityType
     * @return column family name for given entity type
     */
    public static byte[] getColumnFamily(Type entityType) {
        try {
            return entityType.name().getBytes(ENCODING_UTF8);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private InfoSpaceConstants() {
    }
}
