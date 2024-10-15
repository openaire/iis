package pl.edu.icm.coansys.citations;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Lists;

import pl.edu.icm.coansys.citations.data.HeuristicHashMatchingResult;
import pl.edu.icm.coansys.citations.data.IdWithSimilarity;
import pl.edu.icm.coansys.citations.data.MatchableEntity;

/**
 * Core service for citation matching
 * 
 * @author madryk
 */
public class CoreCitationMatchingService {
    
    private DocumentAttacher documentAttacher = new DocumentAttacher();
    
    private CitationAttacherWithMatchedLimiter citationAttacher = new CitationAttacherWithMatchedLimiter();
    
    private BestMatchedCitationPicker bestMatchedCitationPicker = new BestMatchedCitationPicker();
    
    private HeuristicHashCitationMatcherFactory heuristicHashCitationMatcherFactory = new HeuristicHashCitationMatcherFactory();
    
    
    private JavaSparkContext sparkContext;
    
    private List<Pair<MatchableEntityHasher, MatchableEntityHasher>> matchableEntityHashers;
    
    private long maxHashBucketSize;
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns rdd with matched citations
     */
    public JavaPairRDD<MatchableEntity, IdWithSimilarity> matchCitations(JavaPairRDD<String, MatchableEntity> citations, JavaPairRDD<String, MatchableEntity> documents) {
        
        JavaPairRDD<String, String> citIdDocIdPairs = matchCitDocByHashes(citations, documents);
        
        
        JavaPairRDD<String, MatchableEntity> citIdDocPairs = documentAttacher.attachDocuments(citIdDocIdPairs, documents);
        JavaPairRDD<MatchableEntity, MatchableEntity> citDocPairs = citationAttacher.attachCitationsAndLimitDocs(citIdDocPairs, citations);
        
        JavaPairRDD<MatchableEntity, IdWithSimilarity> matchedCitations = bestMatchedCitationPicker.pickBest(citDocPairs);
        
        
        return matchedCitations;
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private JavaPairRDD<String, String> matchCitDocByHashes(
            JavaPairRDD<String, MatchableEntity> citations, JavaPairRDD<String, MatchableEntity> documents) {
        
        JavaPairRDD<String, MatchableEntity> unmatchedCitations = citations;
        JavaPairRDD<String, String> joinedCitDocIdPairs = sparkContext.parallelizePairs(Lists.newArrayList()); // JavaPairRDD.fromJavaRDD(sparkContext.emptyRDD());
        
        Iterator<Pair<MatchableEntityHasher, MatchableEntityHasher>> entitiesHashersIterator = matchableEntityHashers.iterator();
        while(entitiesHashersIterator.hasNext()) {
            Pair<MatchableEntityHasher, MatchableEntityHasher> citAndDocHashers = entitiesHashersIterator.next();
            HeuristicHashCitationMatcher hashHeuristicCitationMatcher = heuristicHashCitationMatcherFactory.create(citAndDocHashers.getLeft(), citAndDocHashers.getRight(), maxHashBucketSize);
            
            HeuristicHashMatchingResult matchedResult = 
                    hashHeuristicCitationMatcher.matchCitations(unmatchedCitations, documents, entitiesHashersIterator.hasNext());
            
            joinedCitDocIdPairs = joinedCitDocIdPairs.union(matchedResult.getCitDocIdPairs());
            unmatchedCitations = matchedResult.getUnmatchedCitations();
        }
        
        return joinedCitDocIdPairs;
    }


    //------------------------ SETTERS --------------------------
    
    public void setDocumentAttacher(DocumentAttacher documentAttacher) {
        this.documentAttacher = documentAttacher;
    }

    public void setCitationAttacher(CitationAttacherWithMatchedLimiter citationAttacher) {
        this.citationAttacher = citationAttacher;
    }

    public void setBestMatchedCitationPicker(BestMatchedCitationPicker bestMatchedCitationPicker) {
        this.bestMatchedCitationPicker = bestMatchedCitationPicker;
    }

    public void setSparkContext(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    public void setMatchableEntityHashers(List<Pair<MatchableEntityHasher, MatchableEntityHasher>> matchableEntityHashers) {
        this.matchableEntityHashers = matchableEntityHashers;
    }

    public void setMaxHashBucketSize(long maxHashBucketSize) {
        this.maxHashBucketSize = maxHashBucketSize;
    }

    public void setHeuristicHashCitationMatcherFactory(
            HeuristicHashCitationMatcherFactory heuristicHashCitationMatcherFactory) {
        this.heuristicHashCitationMatcherFactory = heuristicHashCitationMatcherFactory;
    }
    
}
