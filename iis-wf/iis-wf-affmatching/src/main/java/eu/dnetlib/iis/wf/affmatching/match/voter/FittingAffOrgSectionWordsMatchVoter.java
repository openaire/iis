package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import datafu.com.google.common.base.Objects;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSectionsSplitter;

/**
 * Match voter that checks if {@link AffMatchAffiliation#getOrganizationName()} section 
 * words are present in {@link AffMatchOrganization#getName()} words.
 * 
 * @author madryk
 */
public class FittingAffOrgSectionWordsMatchVoter extends AbstractAffOrgMatchVoter {

    private static final long serialVersionUID = 1L;
    
    
    private OrganizationSectionsSplitter organizationSectionsSplitter = new OrganizationSectionsSplitter();
    
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
     *      to all {@link AffMatchAffiliation#getOrganizationName()} section words.
     *      Value must be between (0,1].
     * @param minFittingWordSimilarity - minimum similarity for two words to be found the same.
     *      Value must be between (0,1] (value equal to one means that two words must be identical).
     *      Similarity is measured by Jaro-Winkler distance algorithm.
     * @see StringUtils#getJaroWinklerDistance(CharSequence, CharSequence)
     */
    public FittingAffOrgSectionWordsMatchVoter(List<Character> charsToFilter, int wordToRemoveMaxLength, 
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
     * are found in any section of {@link AffMatchAffiliation#getOrganizationName()}.
     * 
     * @see #FittingAffWordsMatchVoter(List, int, double, double)
     */
    @Override
    public boolean voteMatch(AffMatchAffiliation affiliation, AffMatchOrganization organization) {
        
        String filteredOrgName = stringFilter.filterCharsAndShortWords(organization.getName(), charsToFilter, wordToRemoveMaxLength);
        
        if (StringUtils.isEmpty(filteredOrgName)) {
            return false;
        }
        
        Set<String> orgWords = Sets.newHashSet(StringUtils.split(filteredOrgName));
        
        
        List<String> affSections = organizationSectionsSplitter.splitToSections(affiliation.getOrganizationName());
        
        for (String affSection : affSections) {
            
            String filteredAffSectionName = stringFilter.filterCharsAndShortWords(affSection, charsToFilter, wordToRemoveMaxLength);
            
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
            if (similarityChecker.containSimilarString(orgWords, affSectionWord, minFittingWordSimilarity)) {
                ++fittingWordsCount;
            }
        }
        
        double fittingWordsRatio = (double)fittingWordsCount/affWordsCount;
        
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
