package eu.dnetlib.iis.wf.affmatching.write;

/**
 * Affiliation matching execution report counter names. 
 * 
 * @author Łukasz Dumiszewski
*/

public final class AffMatchReportCounters {

    public static final String DOC_ORG_REFERENCES = "processing.docOrgMatching.docOrgReferences.affiliationBased";
    public static final String DOCS_WITH_AT_LEAST_ONE_ORG = "processing.docOrgMatching.docs.affiliationBased";
    
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    private AffMatchReportCounters() {
        throw new IllegalStateException("may not be instantiated");
    }
    
}
