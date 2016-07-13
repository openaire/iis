package eu.dnetlib.iis.wf.documentsclassification;

import java.util.Map;

import com.google.common.collect.Maps;

/**
 * Enum that contains counters reported by document classification job.
 * 
 * @author madryk
 */
enum DocumentClassificationCounters {
    
    CLASSIFIED_DOCUMENTS,
    
    ARXIV_CLASSES,
    WOS_CLASSES,
    DDC_CLASSES,
    MESH_EURO_PMC_CLASSES,
    ACM_CLASSES;
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns mapping of document classification job counter names into report param keys.
     */
    public static Map<String, String> getCounterNameToParamKeyMapping() {
        Map<String, String> mapping = Maps.newHashMap();
        
        mapping.put(CLASSIFIED_DOCUMENTS.name(), "export.documentClassification.classifiedDocuments");
        mapping.put(ARXIV_CLASSES.name(), "export.documentClassification.classes.arxiv");
        mapping.put(WOS_CLASSES.name(), "export.documentClassification.classes.wos");
        mapping.put(DDC_CLASSES.name(), "export.documentClassification.classes.ddc");
        mapping.put(MESH_EURO_PMC_CLASSES.name(), "export.documentClassification.classes.meshEuroPmc");
        mapping.put(ACM_CLASSES.name(), "export.documentClassification.classes.acm");
        
        return mapping;
    }
}
