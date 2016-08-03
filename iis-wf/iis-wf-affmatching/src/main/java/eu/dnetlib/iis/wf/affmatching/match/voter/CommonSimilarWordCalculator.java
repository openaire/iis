package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.io.Serializable;
import java.util.List;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Service that calculates the similarity between two collections of words
 * 
 * @author Łukasz Dumiszewski
*/

public class CommonSimilarWordCalculator implements Serializable {


    private static final long serialVersionUID = 1L;
    
    private StringSimilarityChecker similarityChecker = new StringSimilarityChecker();
    
    private double minWordSimilarity;
    
    
    
    //------------------------ CONSTRUCTORS --------------------------
    /** 
     * @param minWordSimilarity - minimum similarity for two words to be found similar.
     *          The value must be between (0,1] (value equal to one means that two words must be identical).
     *          The similarity is measured by Jaro-Winkler distance algorithm.
     *          This property is used in {@link #calcSimilarWordNumber(List, List)}
    */
    public CommonSimilarWordCalculator(double minWordSimilarity) {
        Preconditions.checkArgument(minWordSimilarity > 0 && minWordSimilarity <= 1);
        this.minWordSimilarity = minWordSimilarity;
    }


    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns the number of similar words in the given two lists. Uses {@link #setSimilarityChecker(StringSimilarityChecker)}
     * to decide if two given words are similar.
     */
    public int calcSimilarWordNumber(List<String> findWords, List<String> inWords) {
        
        Preconditions.checkNotNull(findWords);
        Preconditions.checkNotNull(inWords);
        
        int similarWordCount = 0;
        
        for (String word : findWords) {
            if (similarityChecker.containsSimilarString(inWords, word, minWordSimilarity)) {
                ++similarWordCount;
            }
        }
        
        return similarWordCount;
    }
    
    /**
     * Returns the number of the similar words in the given lists WITH REGARD TO the number of
     * the words in the inWords list.
     */
    public double calcSimilarWordRatio(List<String> findWords, List<String> inWords) {
        
        return (double)calcSimilarWordNumber(findWords, inWords) / findWords.size();
    
    }

    
    //------------------------ SETTERS --------------------------
    /**
     * Sets minWordSimilarity - minimum similarity for two words to be found the same.
     * Value must be between (0,1] (value equal to one means that two words must be identical).
     * Similarity is measured by Jaro-Winkler distance algorithm.
     */
    public void setMinWordSimilarity(float minWordSimilarity) {
        Preconditions.checkArgument(minWordSimilarity > 0 && minWordSimilarity <= 1);
        this.minWordSimilarity = minWordSimilarity;
    }
    
    /**
     * Service that will be used to check the similarity of two words
     */
    public void setSimilarityChecker(StringSimilarityChecker similarityChecker) {
        this.similarityChecker = similarityChecker;
    }

    
    //------------------------ toString --------------------------
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("minWordSimilarity", minWordSimilarity)
                                           .toString();
    }



    
}
