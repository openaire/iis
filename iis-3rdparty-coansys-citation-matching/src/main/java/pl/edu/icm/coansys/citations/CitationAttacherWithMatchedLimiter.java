package pl.edu.icm.coansys.citations;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;

import com.google.common.collect.MinMaxPriorityQueue;

import pl.edu.icm.coansys.citations.data.MatchableEntity;
import pl.edu.icm.coansys.citations.util.misc;
import scala.Tuple2;
import scala.collection.JavaConversions;

/**
 * Attacher of citation into (citation_id, document) pairs with limiter of
 * pairs with the same citation_id
 * 
 * @author madryk
 */
public class CitationAttacherWithMatchedLimiter implements Serializable {

    private static final long serialVersionUID = 1L;
    
    
    private final static int DEFAULT_SAME_CITATIONS_LIMIT = 20;
    
    
    private int sameCitationsLimit;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    public CitationAttacherWithMatchedLimiter() {
        this(DEFAULT_SAME_CITATIONS_LIMIT);
    }
    
    public CitationAttacherWithMatchedLimiter(int sameCitationsLimit) {
        this.sameCitationsLimit = sameCitationsLimit;
    }
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Attaches citation into (citation_id, document) pairs.
     * Additionally it limits produced (citation, document) pairs with
     * the same citation to {@link #getSameCitationsLimit()} records.
     * Method limits records based on number of mutual tokens.
     */
    public JavaPairRDD<MatchableEntity, MatchableEntity> attachCitationsAndLimitDocs(JavaPairRDD<String, MatchableEntity> citIdDocPairs, JavaPairRDD<String, MatchableEntity> citations) {
        
        JavaPairRDD<MatchableEntity, MatchableEntity> citDocPairs = citIdDocPairs
                .groupByKey()
                .join(citations)
                .flatMapToPair(docsWithCit -> {
                    
                    MatchableEntity citation = docsWithCit._2._2;
                    
                    Collection<EntityWithSimilarity> docsFiltered = calculateSimilarityAndLimitDocs(citation, docsWithCit._2._1);
                    
                    return docsFiltered.stream()
                            .map(x -> new Tuple2<MatchableEntity, MatchableEntity>(citation, x.getEntity()))
                            .collect(Collectors.toList()).iterator();
                });
        
        return citDocPairs;
    }
    
    
    //------------------------ GETTERS --------------------------
    
    public int getSameCitationsLimit() {
        return sameCitationsLimit;
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private Collection<EntityWithSimilarity> calculateSimilarityAndLimitDocs(MatchableEntity citation, Iterable<MatchableEntity> documents) {
        
        MinMaxPriorityQueue<EntityWithSimilarity> queue = MinMaxPriorityQueue
                .orderedBy(new EntityWithSimilarityComparator())
                .maximumSize(sameCitationsLimit)
                .create();
        
        for (MatchableEntity document : documents) {
            
            double similarity = calculateTokenSimilarity(citation, document);
            
            queue.add(new EntityWithSimilarity(document, similarity));
            
        }
        
        return queue;
    }
    
    private double calculateTokenSimilarity(MatchableEntity citation, MatchableEntity document) {
        
        Set<String> citTokens = JavaConversions.setAsJavaSet(misc.niceTokens(citation.toReferenceString()));
        Set<String> docTokens = JavaConversions.setAsJavaSet(misc.niceTokens(document.toReferenceString()));
        
        long mutualTokensCount = citTokens.stream().filter(x -> docTokens.contains(x)).count();
        
        double similarity = 2.0 * mutualTokensCount / (citTokens.size() + docTokens.size());
        
        return similarity;
    }
    
    
    
    public static class EntityWithSimilarity implements Serializable {
        
        private static final long serialVersionUID = 1L;
        
        
        private MatchableEntity entity;
        private double similarity;
        
        public EntityWithSimilarity(MatchableEntity entity, double similarity) {
            this.entity = entity;
            this.similarity = similarity;
        }

        public double getSimilarity() {
            return similarity;
        }

        public MatchableEntity getEntity() {
            return entity;
        }
        
    }
    
    private static class EntityWithSimilarityComparator implements Comparator<EntityWithSimilarity> {
        
        @Override
        public int compare(EntityWithSimilarity o1, EntityWithSimilarity o2) {
            int similarityCompare = -Double.compare(o1.getSimilarity(), o2.getSimilarity());
            
            if (similarityCompare == 0) {
                return o1.getEntity().id().compareTo(o2.getEntity().id());
            }
            return similarityCompare;
        }
        
    }
}
