package eu.dnetlib.iis.wf.affmatching.write;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

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
     */
    public void write(JavaRDD<AffMatchResult> matchedAffOrgs, String outputPath);
    
}
