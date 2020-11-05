package eu.dnetlib.iis.wf.export.actionmanager;

public class OafConstants {

    public static final String REL_TYPE_RESULT_RESULT = "resultResult";
    public static final String REL_TYPE_RESULT_PROJECT = "resultProject";
    public static final String REL_TYPE_RESULT_ORGANIZATION = "resultOrganization";
    
    public static final String SUBREL_TYPE_RELATIONSHIP = "relationship";
    public static final String SUBREL_TYPE_PUBLICATION_DATASET = "publicationDataset";
    public static final String SUBREL_TYPE_OUTCOME = "outcome";
    public static final String SUBREL_TYPE_AFFILIATION = "affiliation";
    public static final String SUBREL_TYPE_CITATION = "citation";

    public static final String REL_CLASS_ISRELATEDTO = "isRelatedTo";
    public static final String REL_CLASS_ISPRODUCEDBY = "isProducedBy";
    public static final String REL_CLASS_PRODUCES = "produces";
    public static final String REL_CLASS_HAS_AUTHOR_INSTITUTION_OF = "hasAuthorInstitution";
    public static final String REL_CLASS_IS_AUTHOR_INSTITUTION_OF = "isAuthorInstitutionOf";
    public static final String REL_CLASS_CITES = "cites";
    public static final String REL_CLASS_ISCITEDBY = "isCitedBy";

    private OafConstants() {}
    
}
