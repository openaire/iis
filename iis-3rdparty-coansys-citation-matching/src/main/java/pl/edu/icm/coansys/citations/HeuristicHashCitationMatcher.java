package pl.edu.icm.coansys.citations;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;

import pl.edu.icm.coansys.citations.data.HeuristicHashMatchingResult;
import pl.edu.icm.coansys.citations.data.MatchableEntity;
import scala.Tuple2;

/**
 * Citation matcher based on citation and document hash equality.
 * 
 * @author Łukasz Dumiszewski
 *
 */
public class HeuristicHashCitationMatcher {

    private InvalidHashExtractor invalidHashExtractor = new InvalidHashExtractor();
    
    private MatchableEntityHasher citationHasher;
    
    private MatchableEntityHasher documentHasher;
    
    private long maxHashBucketSize;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    public HeuristicHashCitationMatcher(MatchableEntityHasher citationHasher, MatchableEntityHasher documentHasher, long maxHashBucketSize) {
        this.citationHasher = citationHasher;
        this.documentHasher = documentHasher;
        this.maxHashBucketSize = maxHashBucketSize;
    }
    
    
    //------------------------ LOGIC --------------------------
    
    public HeuristicHashMatchingResult matchCitations(JavaPairRDD<String, MatchableEntity> citations, JavaPairRDD<String, MatchableEntity> documents, 
            boolean needUnmatched) {
        
        JavaPairRDD<String, String> citationHashIdPairs = generateHashIdPairs(citations, citationHasher);
        citationHashIdPairs = citationHashIdPairs.partitionBy(new HashPartitioner(citationHashIdPairs.partitions().size()));
        
        JavaPairRDD<String, String> documentHashIdPairs = generateHashIdPairs(documents, documentHasher);
        documentHashIdPairs = documentHashIdPairs.partitionBy(new HashPartitioner(documentHashIdPairs.partitions().size()));

        // remove invalid hashes
        JavaPairRDD<String, Long> invalidHashes = invalidHashExtractor.extractInvalidHashes(citationHashIdPairs, documentHashIdPairs, maxHashBucketSize);

        citationHashIdPairs = citationHashIdPairs.subtractByKey(invalidHashes);
        documentHashIdPairs = documentHashIdPairs.subtractByKey(invalidHashes);
        
        // join citationIds to documentIds by hash
        JavaPairRDD<String, String> citationDocumentIdPairs = citationHashIdPairs.join(documentHashIdPairs).mapToPair(cd->cd._2()).distinct();
        
        // find unmatched citations
        JavaPairRDD<String, MatchableEntity> unmatchedCitations = null;
        if (needUnmatched) {
            unmatchedCitations = citations.subtractByKey(citationDocumentIdPairs);
            
        }
        
        return new HeuristicHashMatchingResult(citationDocumentIdPairs, unmatchedCitations);
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private JavaPairRDD<String, String> generateHashIdPairs(JavaPairRDD<String, MatchableEntity> matchableEntityBytes, MatchableEntityHasher hasher) {
        
        JavaPairRDD<String, String> hashIdPairs = matchableEntityBytes.flatMapToPair((Tuple2<String, MatchableEntity> keyValue)-> {
            MatchableEntity matchableEntity = keyValue._2();
            return hasher.hashEntity(matchableEntity).iterator();
        });
        
        return hashIdPairs;
    }
    
}
