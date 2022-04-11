package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSectionsSplitter;

/**
 * Match voter that checks if <br/>
 * the ratio of the common (same/ similar) words in one of the sections of {@link AffMatchAffiliation#getOrganizationName()} 
 * AND words in organization names in {@link AffMatchOrganization} WITH REGARD TO all the words in the the given affiliation section<br/>
 * IS GREATER than some expected value. 
 *
 * 
 * @author madryk, Łukasz Dumiszewski
 */
public class CommonAffSectionWordsVoter extends AbstractAffOrgMatchVoter {

    private static final long serialVersionUID = 1L;
    
    private static final String COUNTRY_CODE_UNKNOWN = "UNKNOWN";
    
    
    private OrganizationSectionsSplitter organizationSectionsSplitter = new OrganizationSectionsSplitter();
    
    private StringFilter stringFilter = new StringFilter();
    
    private CommonSimilarWordCalculator commonSimilarWordCalculator; 
    
    
    private final List<Character> charsToFilter;
    
    private final double minCommonWordsToAllAffWordsRatio;
    
    private final int wordToRemoveMaxLength;
    
    private final int minNumberOfWordsInAffSection;
    
    private Function<AffMatchOrganization, List<String>> getOrgNamesFunction = new GetOrgNameFunction();
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    /**
     * Default constructor
     * 
     * @param charsToFilter - list of characters that will be filtered out before comparing words
     * @param wordToRemoveMaxLength - words with length equal or less than 
     *      this value will be filtered out before comparing words.
     *      Setting it to zero disables this feature.
     * @param minCommonWordsToAllAffWordsRatio - minimum ratio of {@link AffMatchAffiliation#getOrganizationName()}
     *      section words that are also in {@link AffMatchOrganization#getName()}
     *      to all {@link AffMatchAffiliation#getOrganizationName()} section words.
     *      Value must be between (0,1].
     * @param minNumberOfWordsInAffSection minimum number of words in {@link AffMatchAffiliation#getOrganizationName()} section 
     *      to be considered as a potential match. 
     *
     * @see StringUtils#getJaroWinklerDistance(CharSequence, CharSequence)
     */
    public CommonAffSectionWordsVoter(List<Character> charsToFilter, int wordToRemoveMaxLength, double minCommonWordsToAllAffWordsRatio,
            int minNumberOfWordsInAffSection) {
        
        super();
        
        Preconditions.checkNotNull(charsToFilter);
        
        Preconditions.checkArgument(wordToRemoveMaxLength >= 0);
        
        Preconditions.checkArgument(minCommonWordsToAllAffWordsRatio > 0 && minCommonWordsToAllAffWordsRatio <= 1);
        
        Preconditions.checkArgument(minNumberOfWordsInAffSection > 0);
        
        this.charsToFilter = charsToFilter;
        
        this.wordToRemoveMaxLength = wordToRemoveMaxLength;
        
        this.minCommonWordsToAllAffWordsRatio = minCommonWordsToAllAffWordsRatio;
        
        this.minNumberOfWordsInAffSection = minNumberOfWordsInAffSection;
        
    }
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns true if minCommonWords of the words of at least one of the organization names
     * are found in any section of {@link AffMatchAffiliation#getOrganizationName()}.
     * 
     * @see #CommonAffSectionWordsVoter(List, int, double)
     * @see #setGetOrgNamesFunction(Function)
     */
    @Override
    public boolean voteMatch(AffMatchAffiliation affiliation, AffMatchOrganization organization) {
        
        if (isCountryCodeDefinedAndDifferent(affiliation, organization)) {
            return false;
        }
        
        List<String> affSections = organizationSectionsSplitter.splitToSections(affiliation.getOrganizationName());
        
        for (String orgName : getOrgNamesFunction.apply(organization)) {
        
            String filteredOrgName = stringFilter.filterCharsAndShortWords(orgName, charsToFilter, wordToRemoveMaxLength);
        
            if (StringUtils.isEmpty(filteredOrgName)) {
                continue;
            }
        
            List<String> orgWords = ImmutableList.copyOf(StringUtils.split(filteredOrgName));
        
            if (isAnyAffSectionInOrgWords(affSections, orgWords)) {
                return true;
            }
        }
        
        
        return false;
    }

    //------------------------ PRIVATE --------------------------
    
    private boolean isAnyAffSectionInOrgWords(List<String> affSections, List<String> orgWords) {
        
        for (String affSection : affSections) {
            
            String filteredAffSectionName = stringFilter.filterCharsAndShortWords(affSection, charsToFilter, wordToRemoveMaxLength);
            
            if (StringUtils.isEmpty(filteredAffSectionName)) {
                continue;
            }
            
            List<String> affWords = ImmutableList.copyOf(StringUtils.split(filteredAffSectionName));
            
            if (affWords.size() < minNumberOfWordsInAffSection) {
                continue;
            }
            
            if (voteSectionMatch(affWords, orgWords)) {
                return true;
            }
        }
        
        return false;
    }

    private boolean voteSectionMatch(List<String> affSectionWords, List<String> orgWords) {

        double commonWordsRatio = commonSimilarWordCalculator.calcSimilarWordRatio(affSectionWords, orgWords); 


        return commonWordsRatio >= minCommonWordsToAllAffWordsRatio;
    }
    
    private static boolean isCountryCodeDefinedAndDifferent(AffMatchAffiliation affiliation,
            AffMatchOrganization organization) {
        return StringUtils.isNotBlank(affiliation.getCountryCode())
                && StringUtils.isNotBlank(organization.getCountryCode())
                && !COUNTRY_CODE_UNKNOWN.equals(organization.getCountryCode().toUpperCase())
                && !affiliation.getCountryCode().equals(organization.getCountryCode());
    }
    
    //------------------------ SETTERS --------------------------
    
    /**
     * Sets the function that will be used to get the organization names 
     */
    public void setGetOrgNamesFunction(Function<AffMatchOrganization, List<String>> getOrgNamesFunction) {
        this.getOrgNamesFunction = getOrgNamesFunction;
    }

    
    //------------------------ SETTERS --------------------------

    public void setCommonSimilarWordCalculator(CommonSimilarWordCalculator commonSimilarWordCalculator) {
        this.commonSimilarWordCalculator = commonSimilarWordCalculator;
    }

    
    
    //------------------------ toString --------------------------
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("matchStength", getMatchStrength())
                                           .add("charsToFilter", charsToFilter)
                                           .add("wordToRemoveMaxLength", wordToRemoveMaxLength)
                                           .add("minCommonWordsToAllAffWordsRatio", minCommonWordsToAllAffWordsRatio)
                                           .add("minNumberOfWordsInAffSection", minNumberOfWordsInAffSection)
                                           .add("commonSimilarWordCalculator", commonSimilarWordCalculator)
                                           .add("getOrgNamesFunction", getOrgNamesFunction.getClass().getSimpleName())
                                           .toString();
    }



}
