package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.io.Serializable;
import java.util.List;

import com.google.common.base.Preconditions;

import datafu.com.google.common.base.Objects;

/**
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
    
    
    public int calcSimilarWordNumber(List<String> findWords, List<String> inWords) {
        
        int similarWordCount = 0;
        
        for (String word : findWords) {
            if (similarityChecker.containSimilarString(inWords, word, minWordSimilarity)) {
                ++similarWordCount;
            }
        }
        
        return similarWordCount;
    }
    
    
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
    
    //------------------------ toString --------------------------
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("minWordSimilarity", minWordSimilarity)
                                           .toString();
    }

    
}
