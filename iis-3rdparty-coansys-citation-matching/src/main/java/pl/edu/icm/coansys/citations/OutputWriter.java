package pl.edu.icm.coansys.citations;

import org.apache.spark.api.java.JavaPairRDD;

/**
 * Writer of output matched citations
 * 
 * @author madryk
 */
public interface OutputWriter<K,V> {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Writes output matched citations rdd to path specified as argument.
     */
    public void writeMatchedCitations(JavaPairRDD<K,V> matchedCitations, String path);
    
}
