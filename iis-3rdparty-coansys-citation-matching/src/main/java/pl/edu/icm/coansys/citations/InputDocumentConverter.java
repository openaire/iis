package pl.edu.icm.coansys.citations;

import org.apache.spark.api.java.JavaPairRDD;

import pl.edu.icm.coansys.citations.data.MatchableEntity;

/**
 * Converter of input documents
 * 
 * @author madryk
 */
public interface InputDocumentConverter<K,V> {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns rdd with converted input documents where keys are
     * identifiers and values are {@link MatchableEntity}s
     */
    JavaPairRDD<String, MatchableEntity> convertDocuments(JavaPairRDD<K,V> inputDocuments);
}
