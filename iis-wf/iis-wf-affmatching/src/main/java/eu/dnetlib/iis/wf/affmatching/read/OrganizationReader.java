package eu.dnetlib.iis.wf.affmatching.read;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * Reader of organizations that will be used in affiliation matching.
 * 
 * 
 * @author Łukasz Dumiszewski
*/

public interface OrganizationReader {

    
    /**
     * Reads organizations that are in <code>inputPath</code>. The organizations that are in <code>inputPath</code>
     * can be in any format. The implementation of the reader, however, must return them as rdd of {@link AffMatchOrganization}.  
     */
    public JavaRDD<AffMatchOrganization> readOrganizations(JavaSparkContext sc, String inputPath);

}