package eu.dnetlib.iis.wf.affmatching.write;

import static eu.dnetlib.iis.common.report.ReportEntryFactory.createCounterReportEntry;
import static eu.dnetlib.iis.wf.affmatching.write.AffMatchReportCounters.DOCS_WITH_AT_LEAST_ONE_ORG;
import static eu.dnetlib.iis.wf.affmatching.write.AffMatchReportCounters.DOC_ORG_REFERENCES;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;

/**
 * Generator of the affiliation matching module execution report entries.
 * 
* @author Łukasz Dumiszewski
*/

public class AffMatchReportGenerator implements Serializable {
    

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
