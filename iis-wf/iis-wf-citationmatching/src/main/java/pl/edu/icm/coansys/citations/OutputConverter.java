package pl.edu.icm.coansys.citations;

import org.apache.spark.api.java.JavaPairRDD;

import pl.edu.icm.coansys.citations.data.IdWithSimilarity;
import pl.edu.icm.coansys.citations.data.MatchableEntity;

/**
 * Converter of output matched citations
 * 
 * @author madryk
 */
public interface OutputConverter<K,V> {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns rdd with converted output matched citations
     */
    JavaPairRDD<K, V> convertMatchedCitations(JavaPairRDD<MatchableEntity, IdWithSimilarity> matchedCitations);
}
