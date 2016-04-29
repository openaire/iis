package eu.dnetlib.iis.wf.affmatching.read;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import eu.dnetlib.iis.wf.affmatching.model.DocumentProject;

/**
 * Reader of relations between documents and projects.
 * 
 * @author mhorst
 */

public interface DocumentProjectReader {

	/**
	 * Reads document to project relations that are in <code>inputPath</code>.
	 * The relations that are in <code>inputPath</code> can be in any format.
	 * The implementation of the reader, however, must return them as rdd of
	 * {@link DocumentProject}.
	 */
	public JavaRDD<DocumentProject> readDocumentProject(JavaSparkContext sc, String inputPath);

}