package pl.edu.icm.coansys.citations;

import org.apache.spark.api.java.JavaPairRDD;

/**
* @author Łukasz Dumiszewski
*/

public class InvalidHashExtractor {

    
    /**
     * Extracts hashes that are invalid. The invalid hash is one whose number of appearances in the hashIdPairs1 
     * multiplied by the number in the hashIdPairs2 is bigger than maxHashBucketSize or equal 0 (zero)
     * 
     * @param hashIdPairs1 pair rdd that has tuples containing id and hash
     * @param hashIdPairs2 pair rdd that has tuples containing id and hash 
     * @param maxHashBucketSize max number of pairs of the same hash that have been found in hashIdPairs1 and hashIdPairs2 that will be considered valid
     */
    public JavaPairRDD<String, Long> extractInvalidHashes(JavaPairRDD<String, String> hashIdPairs1, JavaPairRDD<String, String> hashIdPairs2, long maxHashBucketSize) {
        
        JavaPairRDD<String, Long> hash1Count = hashIdPairs1.mapValues(v-> 1l).reduceByKey((x,y)->x+y);
        JavaPairRDD<String, Long> hash2Count = hashIdPairs2.mapValues(v-> 1l).reduceByKey((x,y)->x+y);

        JavaPairRDD<String, Long> hashesSmallerThanBucketSize = hash1Count.cogroup(hash2Count).mapValues(v-> {
            Long hash1IdCount = v._1().iterator().hasNext() ? v._1().iterator().next(): 0l;
            Long hash2IdCount = v._2().iterator().hasNext() ? v._2().iterator().next(): 0l;
            return hash1IdCount*hash2IdCount;
        }).filter(x->(x._2()>maxHashBucketSize || x._2()==0));
        
        return hashesSmallerThanBucketSize;
        
    }
    
}
