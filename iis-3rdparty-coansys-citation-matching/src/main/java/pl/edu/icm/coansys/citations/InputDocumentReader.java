package pl.edu.icm.coansys.citations;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Reader of input documents
 * 
 * @author madryk
 */
public interface InputDocumentReader<K, V> {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns spark rdd containing documents
     * 
     * @param sparkContext - spark context which can be used to create rdd
     * @param inputDocumentPath - path to documents file
     */
    JavaPairRDD<K, V> readDocuments(JavaSparkContext sparkContext, String inputDocumentPath);
    
    
}
