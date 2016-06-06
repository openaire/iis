package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * Match voter that checks if {@link AffMatchOrganization#getName()} words
 * are present in {@link AffMatchAffiliation#getOrganizationName()} words.
 * 
 * @author madryk
 */
public class FittingOrgWordsMatchVoter implements AffOrgMatchVoter {

    private static final long serialVersionUID = 1L;
    
    
    private List<Character> charsToFilter;
    
    private double minFittingOrgWordsPercentage;
    
    private double minFittingWordSimilarity;
    
    private int minWordLength;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    /**
     * Default constructor
     * 
     * @param charsToFilter - list of characters that will be filtered out before comparing words
     * @param minWordLength - words with length equal or less than 
     *      this value will be filtered out before comparing words.
     *      Setting it to zero disables this feature.
     * @param minFittingOrgWordsPercentage - percentage of {@link AffMatchOrganization#getName()}
     *      words that have to be found in {@link AffMatchAffiliation#getOrganizationName()}.
     *      Value must be between (0,1].
     * @param minFittingWordSimilarity - minimum similarity for two words to be found the same.
     *      Value must be between (0,1] (value equal to one means that two words must be identical).
     *      Similarity is measured by Jaro-Winkler distance algorithm.
     * @see StringUtils#getJaroWinklerDistance(CharSequence, CharSequence)
     */
    public FittingOrgWordsMatchVoter(List<Character> charsToFilter, int minWordLength, 
            double minFittingOrgWordsPercentage, double minFittingWordSimilarity) {
        Preconditions.checkNotNull(charsToFilter);
        Preconditions.checkArgument(minWordLength >= 0);
        Preconditions.checkArgument(minFittingOrgWordsPercentage > 0 && minFittingOrgWordsPercentage <= 1);
        Preconditions.checkArgument(minFittingWordSimilarity > 0 && minFittingWordSimilarity <= 1);
        
        
        this.charsToFilter = charsToFilter;
        this.minWordLength = minWordLength;
        this.minFittingOrgWordsPercentage = minFittingOrgWordsPercentage;
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
        
        double fittingWordsPercentage = (double)fittingWordsCount/orgWordsCount;
        
        return fittingWordsPercentage >= minFittingOrgWordsPercentage;
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
        
        if (minWordLength > 0) {
            filteredName = StringUtils.removePattern(filteredName, "\\b\\w{1," + minWordLength + "}\\b");
            filteredName = filteredName.trim().replaceAll(" +", " ");
        }
        
        return filteredName;
    }
}
