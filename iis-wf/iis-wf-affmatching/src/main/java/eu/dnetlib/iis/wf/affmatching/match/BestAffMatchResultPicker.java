package eu.dnetlib.iis.wf.affmatching.match;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import scala.Tuple2;

/**
 * Picker of the best of the affiliation-organization pairs for the given affiliation. 
 * 
 * @author Łukasz Dumiszewski
*/

class BestAffMatchResultPicker implements Serializable {

    
    private static final long serialVersionUID = 1L;

    private AffMatchResultChooser affMatchResultChooser = new AffMatchResultChooser();
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns the best matched affiliation-organization pair for each affiliation, 
     * the one with the biggest match strength ({@link AffMatchResult#getMatchStrength()}. 
     */
    public JavaRDD<AffMatchResult> pickBestAffMatchResults(JavaRDD<AffMatchResult> affMatchResults) {
        
        Preconditions.checkNotNull(affMatchResults);
        
        
        JavaPairRDD<String, AffMatchResult> idAffMatchResults = affMatchResults.mapToPair(r -> new Tuple2<>(r.getAffiliation().getId(), r));
        
        JavaPairRDD<String, AffMatchResult> bestIdAffMatchResults = idAffMatchResults.reduceByKey((r1, r2) -> affMatchResultChooser.chooseBetter(r1, r2));
        
        return bestIdAffMatchResults.values();
    }
        
        
   
}
