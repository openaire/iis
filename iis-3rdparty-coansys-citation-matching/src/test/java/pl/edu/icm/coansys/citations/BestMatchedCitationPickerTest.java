package pl.edu.icm.coansys.citations;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static pl.edu.icm.coansys.citations.MatchableEntityDataProvider.*;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.google.common.collect.ImmutableList;

import pl.edu.icm.coansys.citations.data.IdWithSimilarity;
import pl.edu.icm.coansys.citations.data.MatchableEntity;
import scala.Tuple2;

/**
 * @author madryk
 */
public class BestMatchedCitationPickerTest {

    private final static double DOUBLE_EPSILON = 1e-5;
    
    
    private BestMatchedCitationPicker bestMatchedCitationPicker = new BestMatchedCitationPicker();
    
    private JavaSparkContext sparkContext;
    
    
    @BeforeEach
    public void before() {
        
        SparkConf conf = new SparkConf().setMaster("local").setAppName("BestMatchedCitationPickerTest")
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
    public void pickBest() {
        
        
        // given
        
        JavaPairRDD<MatchableEntity, MatchableEntity> citDocPairs = sparkContext.parallelizePairs(ImmutableList.of(
                new Tuple2<>(citation1, document1),
                new Tuple2<>(citation1, document2),
                new Tuple2<>(citation1, document3),
                new Tuple2<>(citation2, document4),
                new Tuple2<>(citation4, document4),
                new Tuple2<>(citation4, document5)));
        
        
        
        // execute
        
        JavaPairRDD<MatchableEntity, IdWithSimilarity> actualMatchedCitations = bestMatchedCitationPicker.pickBest(citDocPairs);
        
        
        // assert
        
        List<Tuple2<MatchableEntity, IdWithSimilarity>> expectedMatchedCitations = ImmutableList.of(
                new Tuple2<>(citation1, new IdWithSimilarity(document2.id(), 0.748069)),
                new Tuple2<>(citation4, new IdWithSimilarity(document5.id(), 0.739753)));
        
        assertMatchedEquals(actualMatchedCitations.collect(), expectedMatchedCitations);
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void assertMatchedEquals(List<Tuple2<MatchableEntity, IdWithSimilarity>> actualMatchedCitations, List<Tuple2<MatchableEntity, IdWithSimilarity>> expectedMatchedCitations) {
        
        assertEquals(actualMatchedCitations.size(), expectedMatchedCitations.size());
        
        for (Tuple2<MatchableEntity, IdWithSimilarity> expectedMatchedCitation : expectedMatchedCitations) {
            
            String expectedCitId = expectedMatchedCitation._1.id();
            String expectedDocId = expectedMatchedCitation._2.getId();
            
            Tuple2<MatchableEntity, IdWithSimilarity> actualMatchedCitation = findMatchedCitation(actualMatchedCitations, expectedCitId, expectedDocId);
            if (actualMatchedCitation == null) {
                fail("Expected matched citation not found (" + expectedCitId + ", " + expectedDocId + ")");
            }
            
            assertMatchedCitationEquals(expectedMatchedCitation, actualMatchedCitation);
        }
        
    }
    
    private Tuple2<MatchableEntity, IdWithSimilarity> findMatchedCitation(List<Tuple2<MatchableEntity, IdWithSimilarity>> matchedCitations, String citIdToFind, String docIdToFind) {
        
        for (Tuple2<MatchableEntity, IdWithSimilarity> matchedCitation : matchedCitations) {
            
            String citId = matchedCitation._1.id();
            String docId = matchedCitation._2.getId();
            
            if (citId.equals(citIdToFind) && docId.equals(docIdToFind)) {
                return matchedCitation;
            }
            
        }
        return null;
        
    }
    
    private void assertMatchedCitationEquals(Tuple2<MatchableEntity, IdWithSimilarity> expectedMatchedCitation, Tuple2<MatchableEntity, IdWithSimilarity> actualMatchedCitation) {
        
        double expectedSimilarity = expectedMatchedCitation._2.getSimilarity();
        byte[] expectedCitationBytes = expectedMatchedCitation._1.data().toByteArray();
        
        double actualSimilarity = actualMatchedCitation._2.getSimilarity();
        byte[] actualCitationBytes = actualMatchedCitation._1.data().toByteArray();
        
        assertArrayEquals(expectedCitationBytes, actualCitationBytes);
        assertEquals(expectedSimilarity, actualSimilarity, DOUBLE_EPSILON);
    }
    
}
