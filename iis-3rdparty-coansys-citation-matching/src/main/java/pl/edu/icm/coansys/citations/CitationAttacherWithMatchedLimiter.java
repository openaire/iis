package pl.edu.icm.coansys.citations;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;

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
        if (sameCitationsLimit < 0) {
            throw new IllegalArgumentException("sameCitationsLimit must be non-negative, but was: " + sameCitationsLimit);
        }
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
        
        return citIdDocPairs
                .join(citations)
                .mapToPair(t -> new Tuple2<>(t._1,
                        new EntityWithSimilarity(t._2._1,
                                calculateTokenSimilarity(t._2._2, t._2._1))))
                .aggregateByKey(
                        new ArrayList<EntityWithSimilarity>(),
                        (list, es) -> { addToTopK(list, es); return list; },
                        (list1, list2) -> { list2.forEach(es -> addToTopK(list1, es)); return list1; }
                )
                .join(citations)
                .flatMapToPair(t -> {
                        MatchableEntity citation = t._2._2;
                        return t._2._1.stream()
                                .map(es -> new Tuple2<MatchableEntity, MatchableEntity>(citation, es.getEntity()))
                                .iterator();
                });
    }
    
    
    //------------------------ GETTERS --------------------------
    
    public int getSameCitationsLimit() {
        return sameCitationsLimit;
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void addToTopK(List<EntityWithSimilarity> list, EntityWithSimilarity candidate) {
        if (list.size() < sameCitationsLimit) {
            list.add(candidate);
            return;
        }
        EntityWithSimilarityComparator comparator = new EntityWithSimilarityComparator();
        int worstIdx = 0;
        for (int i = 1; i < list.size(); i++) {
            if (comparator.compare(list.get(i), list.get(worstIdx)) > 0) {
                worstIdx = i;
            }
        }
        if (comparator.compare(candidate, list.get(worstIdx)) < 0) {
            list.set(worstIdx, candidate);
        }
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
    
    private static class EntityWithSimilarityComparator implements Comparator<EntityWithSimilarity>, Serializable {
        
        private static final long serialVersionUID = 1L;
        
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
