package eu.dnetlib.iis.wf.affmatching.write;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;

/**
 * Writer of {@link AffMatchResult}s 
 * 
 * @author Łukasz Dumiszewski
*/

public interface AffMatchResultWriter extends Serializable {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Writes the given rdd of {@link AffMatchResult}s under the given path. 
     * Depending on the implementation, the outputReportPath can be used to write some
     * execution reports.
     */
    void write(JavaSparkContext sc, JavaRDD<AffMatchResult> matchedAffOrgs, String outputPath, String outputReportPath, int numberOfEmittedFiles);
    
}
