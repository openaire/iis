package pl.edu.icm.coansys.citations;

import org.apache.spark.api.java.JavaPairRDD;

import pl.edu.icm.coansys.citations.data.MatchableEntity;

/**
 * Converter of input citations
 * 
 * @author madryk
 */
public interface InputCitationConverter<K,V> {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns rdd with converted input citations where keys are
     * identifiers and values are {@link MatchableEntity}s
     */
    JavaPairRDD<String, MatchableEntity> convertCitations(JavaPairRDD<K,V> inputCitations);
    
}
