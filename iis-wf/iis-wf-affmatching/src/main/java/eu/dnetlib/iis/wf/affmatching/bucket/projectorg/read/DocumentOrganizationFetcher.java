package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentOrganization;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchProjectOrganization;

/**
 * Fetcher of document-organization relations ({@link AffMatchDocumentOrganization}).<br />
 * This fetcher internally uses {@link DocumentProjectFetcher} and {@link ProjectOrganizationReader}
 * to get document-project and project-organization relations respectively and then
 * combines them using {@link DocumentOrganizationCombiner}.
 *  
 * 
 * @author madryk
 */
public class DocumentOrganizationFetcher implements Serializable {

    private static final long serialVersionUID = 1L;
    
    
    private ProjectOrganizationReader projectOrganizationReader;
    
    private DocumentProjectFetcher documentProjectFetcher;
    private DocumentOrganizationCombiner documentOrganizationCombiner;
    
    private Float docProjConfidenceLevelThreshold;
    
    
    private transient JavaSparkContext sparkContext;
    
    private String projOrgPath;
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Reads document-project and project-organization relations and combines them into document-organization
     * relations ({@link AffMatchDocumentOrganization}).
     */
    public JavaRDD<AffMatchDocumentOrganization> fetchDocumentOrganizations() {
        
        JavaRDD<AffMatchDocumentProject> docProj = documentProjectFetcher.fetchDocumentProjects();
        
        JavaRDD<AffMatchProjectOrganization> projOrg = projectOrganizationReader.readProjectOrganizations(sparkContext, projOrgPath);
        
        JavaRDD<AffMatchDocumentOrganization> docOrg = documentOrganizationCombiner.combine(docProj, projOrg, docProjConfidenceLevelThreshold);
        
        return docOrg;
    }


    //------------------------ SETTERS --------------------------

    public void setProjectOrganizationReader(ProjectOrganizationReader projectOrganizationReader) {
        this.projectOrganizationReader = projectOrganizationReader;
    }

    public void setDocumentProjectFetcher(DocumentProjectFetcher documentProjectFetcher) {
        this.documentProjectFetcher = documentProjectFetcher;
    }

    public void setDocumentOrganizationCombiner(DocumentOrganizationCombiner documentOrganizationCombiner) {
        this.documentOrganizationCombiner = documentOrganizationCombiner;
    }

    public void setDocProjConfidenceLevelThreshold(Float docProjConfidenceLevelThreshold) {
        this.docProjConfidenceLevelThreshold = docProjConfidenceLevelThreshold;
    }

    public void setSparkContext(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    public void setProjOrgPath(String projOrgPath) {
        this.projOrgPath = projOrgPath;
    }

}
