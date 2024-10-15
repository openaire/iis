package pl.edu.icm.coansys.citations;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;

import com.google.common.collect.Lists;

import pl.edu.icm.coansys.citations.data.IdWithSimilarity;
import pl.edu.icm.coansys.citations.data.MatchableEntity;
import pl.edu.icm.coansys.citations.data.SimilarityMeasurer;
import scala.Tuple2;

/**
 * Picker of best matched citation among (citation, document) pairs.
 * 
 * @author madryk
 */
public class BestMatchedCitationPicker implements Serializable {

    private static final long serialVersionUID = 1L;
    
    
    private static final double MIN_SIMILARITY = 0.5;
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Picks best matched citations among every (citation, document) pair sharing the same citation.
     * Best matched citation is picked based on a similarity between its citation and document.
     * Similarity is calculated using {@link SimilarityMeasurer#similarity(MatchableEntity, MatchableEntity)}.
     * To be included in final results, similarity must be equal or greater than 0.5
     * 
     * @return Rdd with keys being citations and values being ids with similarity
     */
    public JavaPairRDD<MatchableEntity, IdWithSimilarity> pickBest(JavaPairRDD<MatchableEntity, MatchableEntity> citDocPairs) {
        
        JavaPairRDD<MatchableEntity, IdWithSimilarity> bestMatchedCitations = citDocPairs
                .mapPartitionsToPair(citAndDocParitition -> {
                    
                    SimilarityMeasurer similarityMeasurer = new SimilarityMeasurer(SimilarityMeasurer.advancedFvBuilder());
                    
                    return calculateSimilarityAndFilter(similarityMeasurer, citAndDocParitition).iterator();
                })
                .reduceByKey((docWithSim1, docWithSim2) -> (docWithSim1.getSimilarity() > docWithSim2.getSimilarity()) ? docWithSim1 : docWithSim2);

        return bestMatchedCitations;
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private List<Tuple2<MatchableEntity, IdWithSimilarity>> calculateSimilarityAndFilter(SimilarityMeasurer similarityMeasurer, Iterator<Tuple2<MatchableEntity, MatchableEntity>> citAndDocIterable) {
        
        List<Tuple2<MatchableEntity, IdWithSimilarity>> citDocIdPairsWithSimilarity = Lists.newArrayList();
        
        
        while(citAndDocIterable.hasNext()) {
            
            Tuple2<MatchableEntity, MatchableEntity> citAndDoc = citAndDocIterable.next();
            
            double similarity = calculateSimilarity(similarityMeasurer, citAndDoc._1, citAndDoc._2);
            
            if (similarity >= MIN_SIMILARITY) {
                citDocIdPairsWithSimilarity.add(createCitDocWithSimilarityPair(citAndDoc._1, citAndDoc._2, similarity));
            }
        }
        
        return citDocIdPairsWithSimilarity;
    }
    
    private Tuple2<MatchableEntity, IdWithSimilarity> createCitDocWithSimilarityPair(
            MatchableEntity citationWritable, MatchableEntity documentWritable, double similarity) {
        
        return new Tuple2<MatchableEntity, IdWithSimilarity>(
                citationWritable, 
                new IdWithSimilarity(documentWritable.id(), similarity));
    }
    
    private double calculateSimilarity(SimilarityMeasurer similarityMeasurer,
            MatchableEntity citationWritable, MatchableEntity documentWritable) {
        
        MatchableEntity citation = citationWritable;
        MatchableEntity document = documentWritable;
        
        double similarity = similarityMeasurer.similarity(citation, document);
        
        return similarity;
    }

}
