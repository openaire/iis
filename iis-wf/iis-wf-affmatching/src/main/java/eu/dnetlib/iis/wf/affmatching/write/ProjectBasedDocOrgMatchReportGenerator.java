package eu.dnetlib.iis.wf.affmatching.write;

import static eu.dnetlib.iis.common.report.ReportEntryFactory.createCounterReportEntry;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;

/**
 * Generator of the project based doc-org matching module execution report entries.
 * 
* @author mhorst
*/

public class ProjectBasedDocOrgMatchReportGenerator implements Serializable {
    
    public static final String DOC_ORG_REFERENCES = "processing.docOrgMatching.docOrgReferences.projectBased";
    public static final String DOCS_WITH_AT_LEAST_ONE_ORG = "processing.docOrgMatching.docs.projectBased";

    private static final long serialVersionUID = 1L;

    
    //------------------------ LOGIC --------------------------
    
    
    /**
     * Generates execution report entries with the affiliation module execution results.
     * See the {@link AffMatchReportCounters} for the list of generated entries.
     */
    public List<ReportEntry> generateReport(JavaRDD<MatchedOrganization> matchedDocOrganizations) {
        
        Preconditions.checkNotNull(matchedDocOrganizations);
        
        
        List<ReportEntry> reportEntries = Lists.newArrayList();
        
        
        long docOrgReferenceCount = matchedDocOrganizations.count();
        
        reportEntries.add(createCounterReportEntry(DOC_ORG_REFERENCES, docOrgReferenceCount));
        
        
        long docWithAtLeastOneOrgCount = matchedDocOrganizations.map(v->v.getDocumentId()).distinct().count();
        
        reportEntries.add(createCounterReportEntry(DOCS_WITH_AT_LEAST_ONE_ORG, docWithAtLeastOneOrgCount));
                
        return reportEntries;
    }
    
}
