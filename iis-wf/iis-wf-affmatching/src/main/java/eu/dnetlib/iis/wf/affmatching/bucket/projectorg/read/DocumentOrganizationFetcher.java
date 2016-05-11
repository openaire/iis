package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentOrganization;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchProjectOrganization;

/**
 * Fetcher of document-organization relations ({@link AffMatchDocumentOrganization}).<br />
 * This fetcher internally uses {@link DocumentProjectReader} and {@link ProjectOrganizationReader}
 * to read document-project and project-organization relations respectively and then
 * combines them using {@link DocumentOrganizationCombiner}.
 *  
 * 
 * @author madryk
 */
public class DocumentOrganizationFetcher implements Serializable {

    private static final long serialVersionUID = 1L;
    
    
    private DocumentProjectReader documentProjectReader;
    private ProjectOrganizationReader projectOrganizationReader;
    
    private DocumentOrganizationCombiner documentOrganizationCombiner;
    
    private Float docProjConfidenceLevelThreshold;
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Reads document-project and project-organization relations and combines them into document-organization
     * relations ({@link AffMatchDocumentOrganization}).
     */
    public JavaRDD<AffMatchDocumentOrganization> readDocumentOrganization(JavaSparkContext sc, String docProjPath, String projOrgPath) {
        
        JavaRDD<AffMatchDocumentProject> docProj = documentProjectReader.readDocumentProject(sc, docProjPath);
        JavaRDD<AffMatchProjectOrganization> projOrg = projectOrganizationReader.readProjectOrganization(sc, projOrgPath);
        
        JavaRDD<AffMatchDocumentOrganization> docOrg = documentOrganizationCombiner.combine(docProj, projOrg, docProjConfidenceLevelThreshold);
        
        return docOrg;
    }


    //------------------------ SETTERS --------------------------
    
    public void setDocumentProjectReader(DocumentProjectReader documentProjectReader) {
        this.documentProjectReader = documentProjectReader;
    }

    public void setProjectOrganizationReader(ProjectOrganizationReader projectOrganizationReader) {
        this.projectOrganizationReader = projectOrganizationReader;
    }

    public void setDocumentOrganizationCombiner(DocumentOrganizationCombiner documentOrganizationCombiner) {
        this.documentOrganizationCombiner = documentOrganizationCombiner;
    }

    public void setDocProjConfidenceLevelThreshold(Float docProjConfidenceLevelThreshold) {
        this.docProjConfidenceLevelThreshold = docProjConfidenceLevelThreshold;
    }
}
