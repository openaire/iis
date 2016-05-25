package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import scala.Tuple2;

/**
 * Merger of two input {@link AffMatchDocumentProject} rdds.
 * 
 * @author madryk
 */
public class DocumentProjectMerger implements Serializable {

    private static final long serialVersionUID = 1L;


    //------------------------ LOGIC --------------------------
    
    /**
     * Returns merged {@link AffMatchDocumentProject} rdd from two input rdds.<br/>
     * Method returns only unique records (equality of records is checked based on 
     * {@link AffMatchDocumentProject#getDocumentId()} and {@link AffMatchDocumentProject#getProjectId()} fields).<br/>
     * In case of duplicates, the one with higher {@link AffMatchDocumentProject#getConfidenceLevel()}
     * will be picked.
     */
    public JavaRDD<AffMatchDocumentProject> merge(JavaRDD<AffMatchDocumentProject> firstDocumentProjects, JavaRDD<AffMatchDocumentProject> secondDocumentProjects) {
        
        JavaPairRDD<Tuple2<String, String>, AffMatchDocumentProject> firstDocProjWithKey = firstDocumentProjects
                .keyBy(x -> new Tuple2<>(x.getDocumentId(), x.getProjectId()));
        
        JavaPairRDD<Tuple2<String, String>, AffMatchDocumentProject> secondDocProjWithKey = secondDocumentProjects
                .keyBy(x -> new Tuple2<>(x.getDocumentId(), x.getProjectId()));
        
        return firstDocProjWithKey
                .union(secondDocProjWithKey)
                .reduceByKey((x, y) -> x.getConfidenceLevel() >= y.getConfidenceLevel() ? x : y)
                .values();
        
    }
}
