package eu.dnetlib.iis.wf.affmatching.write;

/**
* @author Łukasz Dumiszewski
*/

public final class AffMatchReportCounters {

    public static final String DOC_ORG_REFERENCES = "export.affMatching.docOrgReferences";
    public static final String DOCS_WITH_AT_LEAST_ONE_ORG = "export.affMatching.docsWithAtLeastOneOrg";
    
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    private AffMatchReportCounters() {
        throw new IllegalStateException("may not be instantiated");
    }
    
}
