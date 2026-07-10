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
                .combineByKey(
                        docAndCit -> {
                            List<EntityWithSimilarity> topK = new ArrayList<>(sameCitationsLimit);
                            topK.add(new EntityWithSimilarity(docAndCit._1,
                                    calculateTokenSimilarity(docAndCit._2, docAndCit._1)));
                            return new Tuple2<MatchableEntity, List<EntityWithSimilarity>>(docAndCit._2, topK);
                        },
                        (acc, docAndCit) -> {
                            addToTopK(acc._2, new EntityWithSimilarity(docAndCit._1,
                                    calculateTokenSimilarity(acc._1, docAndCit._1)));
                            return acc;
                        },
                        (acc1, acc2) -> {
                            for (EntityWithSimilarity item : acc2._2) {
                                addToTopK(acc1._2, item);
                            }
                            return acc1;
                        }
                )
                .flatMapToPair(entry ->
                        entry._2._2.stream()
                                .map(x -> new Tuple2<MatchableEntity, MatchableEntity>(entry._2._1, x.getEntity()))
                                .iterator()
                );
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
