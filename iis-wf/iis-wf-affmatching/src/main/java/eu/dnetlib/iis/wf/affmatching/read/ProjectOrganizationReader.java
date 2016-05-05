package eu.dnetlib.iis.wf.affmatching.read;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import eu.dnetlib.iis.wf.affmatching.model.ProjectOrganization;

/**
 * Reader of relations between projects and organizations.
 * 
 * @author mhorst
 */

public interface ProjectOrganizationReader {

    /**
     * Reads project to organization relations that are in <code>inputPath</code>. The relations that are in <code>inputPath</code>
     * can be in any format. The implementation of the reader, however, must return them as rdd of {@link ProjectOrganization}.
     */
    public JavaRDD<ProjectOrganization> readProjectOrganization(JavaSparkContext sc, String inputPath);

}