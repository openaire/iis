package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;

/**
 * Reader of relations between documents and projects.
 * 
 * @author mhorst
 */

public interface DocumentProjectReader extends Serializable {

    /**
     * Reads document to project relations that are in <code>inputPath</code>.
     * The relations that are in <code>inputPath</code> can be in any format.
     * The implementation of the reader, however, must return them as rdd of {@link AffMatchDocumentProject}.
     */
    public JavaRDD<AffMatchDocumentProject> readDocumentProjects(JavaSparkContext sc, String inputPath);

}