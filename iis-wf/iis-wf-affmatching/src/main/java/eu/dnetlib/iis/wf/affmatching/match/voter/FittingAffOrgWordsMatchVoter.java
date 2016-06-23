package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import datafu.com.google.common.base.Objects;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * Match voter that checks if {@link AffMatchAffiliation#getOrganizationName()}  
 * words are present in {@link AffMatchOrganization#getName()} words.
 * 
 * @author madryk, lukdumi
 */
public class FittingAffOrgWordsMatchVoter extends AbstractAffOrgMatchVoter {

    private static final long serialVersionUID = 1L;
    
    private StringFilter stringFilter = new StringFilter();
    
    private StringSimilarityChecker similarityChecker = new StringSimilarityChecker();
    
    private List<Character> charsToFilter;
    
    private double minFittingOrgWordsRatio;
    
    private double minFittingWordSimilarity;
    
    private int wordToRemoveMaxLength;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    /**
     * Default constructor
     * 
     * @param charsToFilter - list of characters that will be filtered out before comparing words
     * @param wordToRemoveMaxLength - words with length equal or less than 
     *      this value will be filtered out before comparing words.
     *      Setting it to zero disables this feature.
     * @param minFittingOrgWordsRatio - minimum ratio of {@link AffMatchAffiliation#getOrganizationName()}
     *      section words that are also in {@link AffMatchOrganization#getName()}
     *      to all {@link AffMatchAffiliation#getOrganizationName()} words.
     *      Value must be between (0,1].
     * @param minFittingWordSimilarity - minimum similarity for two words to be found the same.
     *      Value must be between (0,1] (value equal to one means that two words must be identical).
     *      Similarity is measured by Jaro-Winkler distance algorithm.
     * @see StringUtils#getJaroWinklerDistance(CharSequence, CharSequence)
     */
    public FittingAffOrgWordsMatchVoter(List<Character> charsToFilter, int wordToRemoveMaxLength, double minFittingOrgWordsRatio, double minFittingWordSimilarity) {
        
        Preconditions.checkNotNull(charsToFilter);
        Preconditions.checkArgument(wordToRemoveMaxLength >= 0);
        Preconditions.checkArgument(minFittingOrgWordsRatio > 0 && minFittingOrgWordsRatio <= 1);
        Preconditions.checkArgument(minFittingWordSimilarity > 0 && minFittingWordSimilarity <= 1);
        
        
        this.charsToFilter = charsToFilter;
        this.wordToRemoveMaxLength = wordToRemoveMaxLength;
        this.minFittingOrgWordsRatio = minFittingOrgWordsRatio;
        this.minFittingWordSimilarity = minFittingWordSimilarity;
    }
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns true if minFittingOrgWordsPercentage of {@link AffMatchOrganization#getName()} words
     * are found in any section of {@link AffMatchAffiliation#getOrganizationName()}.
     * 
     * @see #FittingAffWordsMatchVoter(List, int, double, double)
     */
    @Override
    public boolean voteMatch(AffMatchAffiliation affiliation, AffMatchOrganization organization) {
        
        String filteredAffName = stringFilter.filterCharsAndShortWords(affiliation.getOrganizationName(), charsToFilter, wordToRemoveMaxLength);
        String filteredOrgName = stringFilter.filterCharsAndShortWords(organization.getName(), charsToFilter, wordToRemoveMaxLength);
        
        if (StringUtils.isEmpty(filteredAffName) || StringUtils.isEmpty(filteredOrgName)) {
            return false;
        }
        
        Set<String> affWords = Sets.newHashSet(StringUtils.split(filteredAffName));
        Set<String> orgWords = Sets.newHashSet(StringUtils.split(filteredOrgName));
        
        int fittingWordsCount = 0;
        
        for (String orgWord : orgWords) {
            if (similarityChecker.containSimilarString(affWords, orgWord, minFittingWordSimilarity)) {
                ++fittingWordsCount;
            }
        }
        
        double fittingWordsRatio = (double)fittingWordsCount/affWords.size();
        
        return fittingWordsRatio >= minFittingOrgWordsRatio;
    }
    
    
    
    //------------------------ toString --------------------------
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("matchStength", getMatchStrength())
                                           .add("charsToFilter", charsToFilter)
                                           .add("minFittingOrgWordsRatio", minFittingOrgWordsRatio)
                                           .add("minFittingOrgWordSimilarity", minFittingWordSimilarity)
                                           .add("wordToRemoveMaxLength", wordToRemoveMaxLength)
                                           .toString();
    }

}
