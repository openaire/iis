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
 * Match voter that checks if {@link AffMatchOrganization#getName()} words
 * are present in {@link AffMatchAffiliation#getOrganizationName()} words.
 * 
 * @author madryk
 */
public class FittingOrgWordsMatchVoter extends AbstractAffOrgMatchVoter {

    private static final long serialVersionUID = 1L;
    
    
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
     * @param minFittingOrgWordsRatio - minimum ratio of {@link AffMatchOrganization#getName()}
     *      words that have to be found in {@link AffMatchAffiliation#getOrganizationName()}
     *      to all {@link AffMatchOrganization#getName()} words.
     *      Value must be between (0,1].
     * @param minFittingWordSimilarity - minimum similarity for two words to be found the same.
     *      Value must be between (0,1] (value equal to one means that two words must be identical).
     *      Similarity is measured by Jaro-Winkler distance algorithm.
     * @see StringUtils#getJaroWinklerDistance(CharSequence, CharSequence)
     */
    public FittingOrgWordsMatchVoter(List<Character> charsToFilter, int wordToRemoveMaxLength, 
            double minFittingOrgWordsRatio, double minFittingWordSimilarity) {
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
     * are found in {@link AffMatchAffiliation#getOrganizationName()}.
     * 
     * @see #FittingOrgWordsMatchVoter(List, int, double, double)
     */
    @Override
    public boolean voteMatch(AffMatchAffiliation affiliation, AffMatchOrganization organization) {
        
        String filteredAffName = filterName(affiliation.getOrganizationName());
        String filteredOrgName = filterName(organization.getName());
        
        if (StringUtils.isEmpty(filteredAffName) || StringUtils.isEmpty(filteredOrgName)) {
            return false;
        }
        
        Set<String> affWords = Sets.newHashSet(StringUtils.split(filteredAffName));
        Set<String> orgWords = Sets.newHashSet(StringUtils.split(filteredOrgName));
        
        int orgWordsCount = orgWords.size();
        int fittingWordsCount = 0;
        
        for (String orgWord : orgWords) {
            if (wordsContainsSimilarWord(affWords, orgWord)) {
                ++fittingWordsCount;
            }
        }
        
        double fittingWordsRatio = (double)fittingWordsCount/orgWordsCount;
        
        return fittingWordsRatio >= minFittingOrgWordsRatio;
    }

    
    //------------------------ PRIVATE --------------------------
    
    private boolean wordsContainsSimilarWord(Set<String> affWords, String orgWord) {
        
        for (String affWord : affWords) {
            double similarity = StringUtils.getJaroWinklerDistance(affWord, orgWord);
            
            if (similarity >= minFittingWordSimilarity) {
                return true;
            }
        }
        return false;
    }
    
    private String filterName(String name) {
        
        String filteredName = name;
        
        for (Character charToFilter : charsToFilter) {
            filteredName = StringUtils.remove(filteredName, charToFilter);
        }
        
        if (wordToRemoveMaxLength > 0) {
            filteredName = StringUtils.removePattern(filteredName, "\\b\\w{1," + wordToRemoveMaxLength + "}\\b");
            filteredName = filteredName.trim().replaceAll(" +", " ");
        }
        
        return filteredName;
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
