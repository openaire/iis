package pl.edu.icm.coansys.citations;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import scala.Tuple2;

/**
* @author Łukasz Dumiszewski
*/

public class InvalidHashExtractorTest {

    private InvalidHashExtractor invalidHashExtractor = new InvalidHashExtractor();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void extractInvalidHashes() {

        // given
        
        SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("HashBucketLimiterTest");
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            @SuppressWarnings("unchecked")
            List<Tuple2<String, String>> hashes1 = Lists.newArrayList(tuple("1", "x"), tuple("1", "y"), tuple("3", "z"), tuple("1", "x"), tuple("2", "x"), tuple("5", "z"));
            @SuppressWarnings("unchecked")
            List<Tuple2<String, String>> hashes2 = Lists.newArrayList(tuple("1", "x"), tuple("1", "a"), tuple("3", "z"), tuple("3", "aa"), tuple("4", "a"), tuple("5", "z"));
            
            JavaPairRDD<String, String> hash1Rdd = sc.parallelizePairs(hashes1);
            JavaPairRDD<String, String> hash2Rdd = sc.parallelizePairs(hashes2);
            
            // execute
            
            JavaPairRDD<String, Long> invalidHashes = invalidHashExtractor.extractInvalidHashes(hash1Rdd, hash2Rdd, 2);
            
            
            // assert
            
            assertEquals(3, invalidHashes.count());
            checkContains(invalidHashes, "1", 6);
            checkContains(invalidHashes, "2", 0);
            checkContains(invalidHashes, "4", 0);
        }

  }



    
    //------------------------ PRIVATE --------------------------
    
    private void checkContains(JavaPairRDD<String, Long> limitedHashes, String hash, long count) {
        List<Tuple2<String, Long>> filteredList = limitedHashes.collect();
        filteredList = filteredList.stream().filter(x->x._1().equals(hash)).collect(Collectors.toList());
        assertEquals(1, filteredList.size());
        assertEquals(count, filteredList.get(0)._2().longValue());
    }
    
    
    private static Tuple2<String, String> tuple(String key, String value) {
        return new Tuple2<String, String>(key, value);
    }
    
}
