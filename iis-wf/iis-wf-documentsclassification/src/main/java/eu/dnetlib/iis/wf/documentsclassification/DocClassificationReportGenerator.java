package eu.dnetlib.iis.wf.documentsclassification;

import static eu.dnetlib.iis.common.report.ReportEntryFactory.createCounterReportEntry;
import static eu.dnetlib.iis.wf.documentsclassification.DocClassificationReportCounterKeys.ACM_CLASSES;
import static eu.dnetlib.iis.wf.documentsclassification.DocClassificationReportCounterKeys.ARXIV_CLASSES;
import static eu.dnetlib.iis.wf.documentsclassification.DocClassificationReportCounterKeys.CLASSIFIED_DOCUMENTS;
import static eu.dnetlib.iis.wf.documentsclassification.DocClassificationReportCounterKeys.DDC_CLASSES;
import static eu.dnetlib.iis.wf.documentsclassification.DocClassificationReportCounterKeys.MESH_EURO_PMC_CLASSES;
import static eu.dnetlib.iis.wf.documentsclassification.DocClassificationReportCounterKeys.WOS_CLASSES;

import java.util.Collection;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentToDocumentClasses;

/**
 * Service generating a report with document classification job results
 * 
 * @author Łukasz Dumiszewski
*/

class DocClassificationReportGenerator {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Generates the doc classification execution report
     * 
     * @param documentClasses result rdd of the doc classification job execution
     */
    List<ReportEntry> generateReport(JavaRDD<DocumentToDocumentClasses> documentClasses) {
        
        Preconditions.checkNotNull(documentClasses);
        
        long classifiedDocumentCount = documentClasses.count();
        
        if (classifiedDocumentCount == 0) {

            return createReportEntries(0, 0, 0, 0, 0, 0);
        }
        
        
        long arxivClassCount = documentClasses.map(docClass->size(docClass.getClasses().getArXivClasses())).reduce((i1, i2)-> i1+i2);
        
        long wosClassCount = documentClasses.map(docClass->size(docClass.getClasses().getWoSClasses())).reduce((i1, i2)-> i1+i2);
        
        long ddcClassCount = documentClasses.map(docClass->size(docClass.getClasses().getDDCClasses())).reduce((i1, i2)-> i1+i2);
        
        long meshEuroPmcClassCount = documentClasses.map(docClass->size(docClass.getClasses().getMeshEuroPMCClasses())).reduce((i1, i2)-> i1+i2);
        
        long acmClassCount = documentClasses.map(docClass->size(docClass.getClasses().getACMClasses())).reduce((i1, i2)-> i1+i2);
        
        
        
        return createReportEntries(classifiedDocumentCount, arxivClassCount, wosClassCount, ddcClassCount, meshEuroPmcClassCount, acmClassCount);
        
    }

    //------------------------ PRIVATE --------------------------
    
    private ImmutableList<ReportEntry> createReportEntries(long classifiedDocumentCount, long arxivClassCount, long wosClassCount, 
                                                           long ddcClassCount, long meshEuroPmcClassCount, long acmClassCount) {
        
        return ImmutableList.of(createCounterReportEntry(CLASSIFIED_DOCUMENTS, classifiedDocumentCount),
                                createCounterReportEntry(ARXIV_CLASSES, arxivClassCount),
                                createCounterReportEntry(WOS_CLASSES, wosClassCount),
                                createCounterReportEntry(DDC_CLASSES, ddcClassCount),
                                createCounterReportEntry(MESH_EURO_PMC_CLASSES, meshEuroPmcClassCount),
                                createCounterReportEntry(ACM_CLASSES, acmClassCount));
    }
    
    private static long size(Collection<?> collection) { // static - spark will not have to serialize the DocClassificationReportGenerator object
        
        if (collection == null) {
            return 0;
        } 
        
        return collection.size();
    }
    
    
}
