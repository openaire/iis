package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.DocumentOrganizationCombiner;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentOrganization;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchProjectOrganization;

/**
 * 
 * @author madryk
 */
public class DocumentOrganizationReader implements Serializable {

    private static final long serialVersionUID = 1L;
    
    
    private DocumentProjectReader documentProjectReader;
    private ProjectOrganizationReader projectOrganizationReader;
    
    private DocumentOrganizationCombiner documentOrganizationCombiner;
    
    private Float docProjConfidenceLevelThreshold;
    
    
    //------------------------ LOGIC --------------------------
    
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
