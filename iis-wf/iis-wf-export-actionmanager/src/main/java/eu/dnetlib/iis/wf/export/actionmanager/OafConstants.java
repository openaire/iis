package eu.dnetlib.iis.wf.export.actionmanager;

import eu.dnetlib.dhp.schema.common.ModelConstants;

public class OafConstants {

    public static final String REL_TYPE_RESULT_RESULT = ModelConstants.RESULT_RESULT;
    public static final String REL_TYPE_RESULT_PROJECT = ModelConstants.RESULT_PROJECT;
    public static final String REL_TYPE_RESULT_ORGANIZATION = ModelConstants.RESULT_ORGANIZATION;
    public static final String REL_TYPE_RESULT_SERVICE = "resultService";
    
    public static final String SUBREL_TYPE_RELATIONSHIP = ModelConstants.RELATIONSHIP;
    public static final String SUBREL_TYPE_OUTCOME = ModelConstants.OUTCOME;
    public static final String SUBREL_TYPE_AFFILIATION = ModelConstants.AFFILIATION;
    public static final String SUBREL_TYPE_CITATION = ModelConstants.CITATION;
    public static final String SUBREL_TYPE_SIMILARITY = ModelConstants.SIMILARITY;

    public static final String REL_CLASS_ISRELATEDTO = ModelConstants.IS_RELATED_TO;
    public static final String REL_CLASS_REFERENCES = ModelConstants.REFERENCES;
    public static final String REL_CLASS_IS_REFERENCED_BY = ModelConstants.IS_REFERENCED_BY;
    public static final String REL_CLASS_ISPRODUCEDBY = ModelConstants.IS_PRODUCED_BY;
    public static final String REL_CLASS_PRODUCES = ModelConstants.PRODUCES;
    public static final String REL_CLASS_HAS_AUTHOR_INSTITUTION_OF = ModelConstants.HAS_AUTHOR_INSTITUTION;
    public static final String REL_CLASS_IS_AUTHOR_INSTITUTION_OF = ModelConstants.IS_AUTHOR_INSTITUTION_OF;
    public static final String REL_CLASS_CITES = ModelConstants.CITES;
    public static final String REL_CLASS_ISCITEDBY = ModelConstants.IS_CITED_BY;
    public static final String REL_CLASS_IS_AMONG_TOP_N = ModelConstants.IS_AMONG_TOP_N_SIMILAR_DOCS;
    public static final String REL_CLASS_HAS_AMONG_TOP_N = ModelConstants.HAS_AMONG_TOP_N_SIMILAR_DOCS;

    private OafConstants() {}
    
}
