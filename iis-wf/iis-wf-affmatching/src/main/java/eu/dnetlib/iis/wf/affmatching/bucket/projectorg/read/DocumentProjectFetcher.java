package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;

/**
 * Fetcher of document-project relations ({@link AffMatchDocumentProject}) using two input sources.<br />
 * This fetcher internally uses two {@link DocumentProjectReader}s to read 
 * document-project relations located under different paths and then
 * merges them using {@link DocumentProjectMerger}.
 * 
 * @author madryk
 */
public class DocumentProjectFetcher implements Serializable {

    private static final long serialVersionUID = 1L;

    
    private DocumentProjectReader firstDocumentProjectReader;
    
    private DocumentProjectReader secondDocumentProjectReader;
    
    private DocumentProjectMerger documentProjectMerger;
    
    
    private transient JavaSparkContext sparkContext;
    
    private String firstDocProjPath;
    
    private String secondDocProjPath;
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * 
     * Returns merged document-project relations that was read from two different paths.
     */
    public JavaRDD<AffMatchDocumentProject> fetchDocumentProjects() {
        
        JavaRDD<AffMatchDocumentProject> firstDocumentProjects = firstDocumentProjectReader.readDocumentProjects(sparkContext, firstDocProjPath);
        JavaRDD<AffMatchDocumentProject> secondDocumentProjects = secondDocumentProjectReader.readDocumentProjects(sparkContext, secondDocProjPath);
        
        JavaRDD<AffMatchDocumentProject> mergedDocumentProjects = documentProjectMerger.merge(firstDocumentProjects, secondDocumentProjects);
        
        
        return mergedDocumentProjects;
    }


    //------------------------ SETTERS --------------------------
    
    public void setFirstDocumentProjectReader(DocumentProjectReader firstDocumentProjectReader) {
        this.firstDocumentProjectReader = firstDocumentProjectReader;
    }

    public void setSecondDocumentProjectReader(DocumentProjectReader secondDocumentProjectReader) {
        this.secondDocumentProjectReader = secondDocumentProjectReader;
    }

    public void setDocumentProjectMerger(DocumentProjectMerger documentProjectMerger) {
        this.documentProjectMerger = documentProjectMerger;
    }

    public void setSparkContext(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    public void setFirstDocProjPath(String firstDocProjPath) {
        this.firstDocProjPath = firstDocProjPath;
    }

    public void setSecondDocProjPath(String secondDocProjPath) {
        this.secondDocProjPath = secondDocProjPath;
    }
}
