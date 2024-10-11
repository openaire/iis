package pl.edu.icm.coansys.citations;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Reader of input citations
 * 
 * @author madryk
 */
public interface InputCitationReader<K,V> {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns spark rdd containing citations
     * 
     * @param sparkContext - spark context which can be used to create rdd
     * @param inputCitationPath - path to citations file
     */
    JavaPairRDD<K,V> readCitations(JavaSparkContext sparkContext, String inputCitationPath);
    
    
}
