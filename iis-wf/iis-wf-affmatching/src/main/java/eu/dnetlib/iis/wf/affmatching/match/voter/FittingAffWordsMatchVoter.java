package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSectionsSplitter;

/**
 * Match voter that checks if {@link AffMatchAffiliation#getOrganizationName()} section 
 * words are present in {@link AffMatchOrganization#getName()} words.
 * 
 * @author madryk
 */
public class FittingAffWordsMatchVoter implements AffOrgMatchVoter {

    private static final long serialVersionUID = 1L;
    
    
    private OrganizationSectionsSplitter organizationSectionsSplitter = new OrganizationSectionsSplitter();
    
    private List<Character> charsToFilter;
    
    private double minFittingOrgWordsRatio;
    
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
     * @param minFittingOrgWordsRatio - minimum ratio of {@link AffMatchAffiliation#getOrganizationName()}
     *      section words that have to be found in {@link AffMatchOrganization#getName()}
     *      to all {@link AffMatchAffiliation#getOrganizationName()} section words.
     *      Value must be between (0,1].
     * @param minFittingWordSimilarity - minimum similarity for two words to be found the same.
     *      Value must be between (0,1] (value equal to one means that two words must be identical).
     *      Similarity is measured by Jaro-Winkler distance algorithm.
     * @see StringUtils#getJaroWinklerDistance(CharSequence, CharSequence)
     */
    public FittingAffWordsMatchVoter(List<Character> charsToFilter, int minWordLength, 
            double minFittingOrgWordsRatio, double minFittingWordSimilarity) {
        Preconditions.checkNotNull(charsToFilter);
        Preconditions.checkArgument(minWordLength >= 0);
        Preconditions.checkArgument(minFittingOrgWordsRatio > 0 && minFittingOrgWordsRatio <= 1);
        Preconditions.checkArgument(minFittingWordSimilarity > 0 && minFittingWordSimilarity <= 1);
        
        
        this.charsToFilter = charsToFilter;
        this.minWordLength = minWordLength;
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
        
        List<String> affSections = organizationSectionsSplitter.splitToSections(affiliation.getOrganizationName());
        
        
        String filteredOrgName = filterName(organization.getName());
        
        if (StringUtils.isEmpty(filteredOrgName)) {
            return false;
        }
        
        Set<String> orgWords = Sets.newHashSet(StringUtils.split(filteredOrgName));
        
        
        for (String affSection : affSections) {
            
            String filteredAffSectionName = filterName(affSection);
            
            if (StringUtils.isEmpty(filteredAffSectionName)) {
                continue;
            }
            
            Set<String> affWords = Sets.newHashSet(StringUtils.split(filteredAffSectionName));
            
            if (voteSectionMatch(affWords, orgWords)) {
                return true;
            }
        }
        
        return false;
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private boolean voteSectionMatch(Set<String> affSectionWords, Set<String> orgWords) {
        int affWordsCount = affSectionWords.size();
        int fittingWordsCount = 0;
        
        for (String affSectionWord : affSectionWords) {
            if (wordsContainsSimilarWord(orgWords, affSectionWord)) {
                ++fittingWordsCount;
            }
        }
        
        double fittingWordsRatio = (double)fittingWordsCount/affWordsCount;
        
        return fittingWordsRatio >= minFittingOrgWordsRatio;
    }

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
