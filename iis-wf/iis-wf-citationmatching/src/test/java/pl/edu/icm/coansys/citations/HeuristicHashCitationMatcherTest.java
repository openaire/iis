
package pl.edu.icm.coansys.citations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import pl.edu.icm.coansys.citations.HeuristicHashCitationMatcher;
import pl.edu.icm.coansys.citations.MatchableEntityHasher;
import pl.edu.icm.coansys.citations.data.HeuristicHashMatchingResult;
import pl.edu.icm.coansys.citations.data.MatchableEntity;
import pl.edu.icm.coansys.citations.hashers.CitationNameYearHashGenerator;
import pl.edu.icm.coansys.citations.hashers.CitationNameYearPagesHashGenerator;
import pl.edu.icm.coansys.citations.hashers.DocumentNameYearHashGenerator;
import pl.edu.icm.coansys.citations.hashers.DocumentNameYearNumNumHashGenerator;
import pl.edu.icm.coansys.citations.hashers.DocumentNameYearPagesHashGenerator;
import pl.edu.icm.coansys.citations.hashers.DocumentNameYearStrictHashGenerator;
import pl.edu.icm.coansys.citations.hashers.HashGenerator;
import pl.edu.icm.coansys.commons.hadoop.LocalSequenceFileUtils;
import scala.Tuple2;


/**
 * @author Łukasz Dumiszewski
 * @author madryk
 */
public class HeuristicHashCitationMatcherTest {
    
    
    private JavaSparkContext sparkContext;
    
    
    @BeforeEach
    public void before() {
        
        SparkConf conf = new SparkConf().setMaster("local").setAppName("HeuristicHashCitationMatcherTest")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "pl.edu.icm.coansys.citations.MatchableEntityKryoRegistrator");
        
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
    public void matchCitations_1() throws IOException {
        
        
        // given
        
        String citationPath = "src/test/resources/heuristic/citations";
        
        String documentPath = "src/test/resources/heuristic/documents";
        
        HeuristicHashCitationMatcher heuristicMatcher = new HeuristicHashCitationMatcher(
                createMatchableEntityHasher(new CitationNameYearPagesHashGenerator()),
                createMatchableEntityHasher(new DocumentNameYearPagesHashGenerator()), 10000);
        
        
        JavaPairRDD<String, MatchableEntity> citations = loadEntities(sparkContext, citationPath);
        
        JavaPairRDD<String, MatchableEntity> documents = loadEntities(sparkContext, documentPath);
        
        
        
        // execute
        
        HeuristicHashMatchingResult matchedResult = heuristicMatcher.matchCitations(citations, documents, true);
        
        
        // assert
        
        assertMatchedCitations(matchedResult.getCitDocIdPairs().collect(), "src/test/resources/heuristic/heuristic-out-1");
        
        assertUnmatchedCitations(matchedResult.getUnmatchedCitations().collect(), "src/test/resources/heuristic/unmatched-1");
        
    }

    
    @Test
    public void matchCitations_2() throws IOException {
        
        
        // given
        
        String citationPath = "src/test/resources/heuristic/unmatched-1";
        
        String documentPath = "src/test/resources/heuristic/documents";
        
        HeuristicHashCitationMatcher heuristicMatcher = new HeuristicHashCitationMatcher(
                createMatchableEntityHasher(new CitationNameYearPagesHashGenerator()), 
                createMatchableEntityHasher(new DocumentNameYearNumNumHashGenerator()), 10000);
        
        
        JavaPairRDD<String, MatchableEntity> citations = loadEntities(sparkContext, citationPath);
        
        JavaPairRDD<String, MatchableEntity> documents = loadEntities(sparkContext, documentPath);
        
        
        
        // execute
        
        HeuristicHashMatchingResult matchedResult = heuristicMatcher.matchCitations(citations, documents, true);
        
        
        // assert
        
        assertMatchedCitations(matchedResult.getCitDocIdPairs().collect(), "src/test/resources/heuristic/heuristic-out-2");
        
        assertUnmatchedCitations(matchedResult.getUnmatchedCitations().collect(), "src/test/resources/heuristic/unmatched-2");
        
    }
    
    
    @Test
    public void matchCitations_3() throws IOException {
        
        
        // given
        
        String citationPath = "src/test/resources/heuristic/unmatched-2";
        
        String documentPath = "src/test/resources/heuristic/documents";
        
        HeuristicHashCitationMatcher heuristicMatcher = new HeuristicHashCitationMatcher(
                createMatchableEntityHasher(new CitationNameYearHashGenerator()), 
                createMatchableEntityHasher(new DocumentNameYearStrictHashGenerator()), 10000);
        
        
        JavaPairRDD<String, MatchableEntity> citations = loadEntities(sparkContext, citationPath);
        
        JavaPairRDD<String, MatchableEntity> documents = loadEntities(sparkContext, documentPath);
        
        
        // execute
        
        HeuristicHashMatchingResult matchedResult = heuristicMatcher.matchCitations(citations, documents, true);
        
        
        // assert
        
        assertMatchedCitations(matchedResult.getCitDocIdPairs().collect(), "src/test/resources/heuristic/heuristic-out-3");
        
        assertUnmatchedCitations(matchedResult.getUnmatchedCitations().collect(), "src/test/resources/heuristic/unmatched-3");
        
    }
    
    
    @Test
    public void matchCitations_4() throws IOException {
        
        
        // given
        
        String citationPath = "src/test/resources/heuristic/unmatched-3";
        
        String documentPath = "src/test/resources/heuristic/documents";
        
        HeuristicHashCitationMatcher heuristicMatcher = new HeuristicHashCitationMatcher(
                createMatchableEntityHasher(new CitationNameYearHashGenerator()), 
                createMatchableEntityHasher(new DocumentNameYearHashGenerator()), 10000);
        
        
        JavaPairRDD<String, MatchableEntity> citations = loadEntities(sparkContext, citationPath);
        
        JavaPairRDD<String, MatchableEntity> documents = loadEntities(sparkContext, documentPath);
        
        
        // execute
        
        HeuristicHashMatchingResult matchedResult = heuristicMatcher.matchCitations(citations, documents, false);
        
        
        // assert
        
        assertMatchedCitations(matchedResult.getCitDocIdPairs().collect(), "src/test/resources/heuristic/heuristic-out-4");
        
        assertNull(matchedResult.getUnmatchedCitations());
        
    }


    
    
    //------------------------ PRIVATE --------------------------

    private static JavaPairRDD<String, MatchableEntity> loadEntities(JavaSparkContext sc, String entitiesFilePath) {
        
        JavaPairRDD<String, MatchableEntity> entities = sc.sequenceFile(entitiesFilePath, Text.class, BytesWritable.class)
                .mapToPair(x -> new Tuple2<String, MatchableEntity>(x._1.toString(), MatchableEntity.fromBytes(x._2.copyBytes())));
        
        return entities;
    }
    
    private void assertUnmatchedCitations(List<Tuple2<String, MatchableEntity>> actualUnmatchedCitations, String expectedUnmatchedCitationDirPath) throws IOException {
        
        List<Pair<String, MatchableEntity>> expectedUnmatchedCitations = readUnmatchedCitationsFile(expectedUnmatchedCitationDirPath);
        
        assertEquals(expectedUnmatchedCitations.size(), actualUnmatchedCitations.size());
        
        for (Tuple2<String, MatchableEntity> actualUnmatchedCitation : actualUnmatchedCitations) {
            assertTrue(isInUnmatchedCitations(expectedUnmatchedCitations, actualUnmatchedCitation));
        }
        
    }
    
    
    private void assertMatchedCitations(List<Tuple2<String, String>> actualMatchedCitations, String expectedMatchedCitationDirPath) throws IOException {
        
        List<Pair<String, String>> expectedMatchedCitations = readMatchedCitationsFile(expectedMatchedCitationDirPath);
        
        assertEquals(expectedMatchedCitations.size(), actualMatchedCitations.size());
        
        for (Tuple2<String, String> actualCitationDocIdPair : actualMatchedCitations) {
            assertTrue(isInMatchedCitations(expectedMatchedCitations, actualCitationDocIdPair));
        }
    }
    
    
    private boolean isInMatchedCitations(List<Pair<String, String>> citations, Tuple2<String, String> citationDocIdPair) {
        
        for (Pair<String, String> citDocIdPair : citations) { 
            if (citDocIdPair.getKey().equals(citationDocIdPair._1) && (citDocIdPair.getValue().equals(citationDocIdPair._2))) {
                return true;
            }
        }
        
        return false;
        
    }

    private boolean isInUnmatchedCitations(List<Pair<String, MatchableEntity>> citations, Tuple2<String, MatchableEntity> citation) {
        
        for (Pair<String, MatchableEntity> cit : citations) {
            if (cit.getKey().equals(citation._1)) {
                return Arrays.equals(cit.getValue().data().toByteArray(), citation._2.data().toByteArray());
            }
                
        }
        
        return false;
        
    }
    
    private MatchableEntityHasher createMatchableEntityHasher(HashGenerator hashGenerator) {
        MatchableEntityHasher matchableEntityHasher = new MatchableEntityHasher();
        matchableEntityHasher.setHashGenerator(hashGenerator);
        return matchableEntityHasher;
    }
    
    
    
    private List<Pair<String, MatchableEntity>> readUnmatchedCitationsFile(String path) throws IOException {
        
        List<Pair<Text, BytesWritable>> unmatchedWritable = LocalSequenceFileUtils.readSequenceFile(new File(path), Text.class, BytesWritable.class);
        
        return unmatchedWritable.stream()
                .map(pair -> new ImmutablePair<>(
                        pair.getKey().toString(),
                        MatchableEntity.fromBytes(pair.getValue().copyBytes())))
                .collect(Collectors.toList());
        
    }
    
    private List<Pair<String, String>> readMatchedCitationsFile(String path) throws IOException {
        
        List<Pair<Text, Text>> matchedCitationsWritable = LocalSequenceFileUtils.readSequenceFile(new File(path), Text.class, Text.class);
        
        return matchedCitationsWritable.stream()
                .map(pair -> new ImmutablePair<>(
                        pair.getKey().toString(),
                        pair.getValue().toString()))
                .collect(Collectors.toList());
        
    }


    
    
    
}
