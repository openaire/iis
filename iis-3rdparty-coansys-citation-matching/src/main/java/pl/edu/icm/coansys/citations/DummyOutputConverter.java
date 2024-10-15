package pl.edu.icm.coansys.citations;

import org.apache.spark.api.java.JavaPairRDD;

import pl.edu.icm.coansys.citations.data.IdWithSimilarity;
import pl.edu.icm.coansys.citations.data.MatchableEntity;

/**
 * Dummy converter of output matched citations.
 * It has empty implementation of converting matched citations.
 * 
 * @author madryk
 */
public class DummyOutputConverter implements OutputConverter<MatchableEntity, IdWithSimilarity> {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns the same rdd that was specified as argument.
     */
    @Override
    public JavaPairRDD<MatchableEntity, IdWithSimilarity> convertMatchedCitations(
            JavaPairRDD<MatchableEntity, IdWithSimilarity> matchedCitations) {
        return matchedCitations;
    }

}
