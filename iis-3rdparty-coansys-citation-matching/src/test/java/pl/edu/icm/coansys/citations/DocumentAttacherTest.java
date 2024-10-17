package pl.edu.icm.coansys.citations;

import static org.junit.jupiter.api.Assertions.*;
import static pl.edu.icm.coansys.citations.MatchableEntityDataProvider.*;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import pl.edu.icm.coansys.citations.data.MatchableEntity;
import scala.Tuple2;

/**
 * @author madryk
 */
public class DocumentAttacherTest {
    
    private DocumentAttacher documentAttacher = new DocumentAttacher();
    
    private JavaSparkContext sparkContext;
    
    
    @BeforeEach
    public void before() {
        
        SparkConf conf = new SparkConf().setMaster("local").setAppName("DocumentAttacherTest")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        
        sparkContext = new JavaSparkContext(conf);
        
    }
    
    
    @AfterEach
    public void after() {
        
        if (sparkContext != null) {
            sparkContext.close();
            sparkContext = null;
        }
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void attachDocuments() {
        
        
        // given
        
        JavaPairRDD<String, String> matchedCitations = sparkContext.parallelizePairs(ImmutableList.of(
                new Tuple2<>(citation1.id(), document1.id()),
                new Tuple2<>(citation1.id(), document2.id()),
                new Tuple2<>(citation4.id(), document2.id()),
                new Tuple2<>(citation4.id(), document5.id())));
        
        JavaPairRDD<String, MatchableEntity> documents = sparkContext.parallelizePairs(generateIdWithEntityTuples(
                Lists.newArrayList(document1, document2, document3, document4, document5)));
        
        
        
        // execute
        
        JavaPairRDD<String, MatchableEntity> actualCitIdDocPairs = documentAttacher.attachDocuments(matchedCitations, documents);
        
        
        // assert
        List<Tuple2<String, MatchableEntity>> expectedCitIdDocPairs = ImmutableList.of(
                new Tuple2<>(citation1.id(), document1),
                new Tuple2<>(citation1.id(), document2),
                new Tuple2<>(citation4.id(), document2),
                new Tuple2<>(citation4.id(), document5));
        
        assertDocAttachedMatchedCitations(actualCitIdDocPairs.collect(), expectedCitIdDocPairs);
        
    }
    
    @Test
    public void attachDocuments_EMPTY_DOCS() {
        
        
        // given
        
        JavaPairRDD<String, String> matchedCitations = sparkContext.parallelizePairs(ImmutableList.of(
                new Tuple2<>(citation1.id(), document1.id()),
                new Tuple2<>(citation1.id(), document2.id()),
                new Tuple2<>(citation4.id(), document5.id())));
        
        JavaPairRDD<String, MatchableEntity> documents = sparkContext.parallelizePairs(Lists.newArrayList());
        
        
        
        // execute
        
        JavaPairRDD<String, MatchableEntity> actualCitIdDocPairs = documentAttacher.attachDocuments(matchedCitations, documents);
        
        
        // assert
        
        assertEquals(actualCitIdDocPairs.count(), 0);
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void assertDocAttachedMatchedCitations(List<Tuple2<String, MatchableEntity>> actualCitIdDocPairs, List<Tuple2<String, MatchableEntity>> expectedCitIdDocPairs) {
        
        assertEquals(actualCitIdDocPairs.size(), expectedCitIdDocPairs.size());
        
        for (Tuple2<String, MatchableEntity> actualCitIdDocPair : actualCitIdDocPairs) {
            assertTrue(isInCitIdDocPairs(expectedCitIdDocPairs, actualCitIdDocPair));
        }
    }
    
    
    private boolean isInCitIdDocPairs(List<Tuple2<String, MatchableEntity>> citIdDocPairs, Tuple2<String, MatchableEntity> citIdDocPairToFind) {
        
        String citIdToFind = citIdDocPairToFind._1;
        String docIdToFind = citIdDocPairToFind._2.id();
        
        for (Tuple2<String, MatchableEntity> citIdDocPair : citIdDocPairs) {
            
            String citId = citIdDocPair._1;
            String docId = citIdDocPair._2.id();
            
            if (citId.equals(citIdToFind) && docId.equals(docIdToFind)) {
                return Arrays.equals(citIdDocPair._2.data().toByteArray(), citIdDocPairToFind._2.data().toByteArray());
            }
        }
        
        return false;
        
    }
}
