package pl.edu.icm.coansys.citations;

/**
 * Factory of {@link HeuristicHashCitationMatcher} objects
 * 
 * @author madryk
 */
public class HeuristicHashCitationMatcherFactory {

    /**
     * Creates {@link HeuristicHashCitationMatcher} object
     */
    public HeuristicHashCitationMatcher create(MatchableEntityHasher citationHasher, MatchableEntityHasher documentHasher, long maxHashBucketSize) {
        
        HeuristicHashCitationMatcher heuristicHashCitationMatcher = new HeuristicHashCitationMatcher(citationHasher, documentHasher, maxHashBucketSize);
        
        return heuristicHashCitationMatcher;
    }
}
