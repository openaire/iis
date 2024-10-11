package pl.edu.icm.coansys.citations.data;

import org.apache.spark.api.java.JavaPairRDD;

/**
 * Result of {@link pl.edu.icm.coansys.citations.HeuristicHashCitationMatcher}.
 * It contains matched citations and documents id pairs and
 * citations that was unmatched.
 * 
 * @author madryk
 *
 */
public class HeuristicHashMatchingResult {

    private JavaPairRDD<String, String> citDocIdPairs;
    
    private JavaPairRDD<String, MatchableEntity> unmatchedCitations;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    public HeuristicHashMatchingResult(JavaPairRDD<String, String> citDocIdPairs, JavaPairRDD<String, MatchableEntity> unmatchedCitations) {
        this.citDocIdPairs = citDocIdPairs;
        this.unmatchedCitations = unmatchedCitations;
    }
    
    
    //------------------------ GETTERS --------------------------
    
    public JavaPairRDD<String, String> getCitDocIdPairs() {
        return citDocIdPairs;
    }
    
    public JavaPairRDD<String, MatchableEntity> getUnmatchedCitations() {
        return unmatchedCitations;
    }
}
