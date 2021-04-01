package eu.dnetlib.iis.wf.affmatching.match;

import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createNameCountryStrictMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createNameStrictCountryLooseMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createSectionedNameLevenshteinCountryLooseMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createSectionedNameStrictCountryLooseMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.CommonWordsVoter.RatioRelation.WITH_REGARD_TO_AFF_WORDS;
import static eu.dnetlib.iis.wf.affmatching.match.voter.CommonWordsVoter.RatioRelation.WITH_REGARD_TO_ORG_WORDS;

import java.util.List;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;

import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgHashBucketJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.OrganizationNameBucketHasher;
import eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CommonSimilarWordCalculator;
import eu.dnetlib.iis.wf.affmatching.match.voter.CommonWordsVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CompositeMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CountryCodeLooseMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgNameFunction;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgShortNameFunction;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * A factory of {@link AffOrgMatcher}s that join organizations and affiliations into buckets based on hashes produced from
 * first words of organization names. 
 * @author Łukasz Dumiszewski
*/

public final class FirstWordsHashBucketMatcherFactory {

    // ---------------------- CONSTRUCTORS ----------------------
    
    private FirstWordsHashBucketMatcherFactory() {}
    
    // ---------------------- LOGIC -----------------------------
    
    /**
     * Returns {@link AffOrgMatcher} that uses hashing of affiliations and organizations to create buckets.<br/>
     * Hashes are computed based on first letters of first words of {@link AffMatchAffiliation#getOrganizationName()}
     * and {@link AffMatchOrganization#getName()}.
     */
    public static AffOrgMatcher createNameFirstWordsHashBucketMatcher() {
        
        // joiner
        
        AffOrgHashBucketJoiner firstWordsHashBucketJoiner = createFirstWordsAffOrgHashBucketJoiner(new GetOrgNameFunction());
        
        // computer
        
        AffOrgMatchComputer firstWordsHashMatchComputer = new AffOrgMatchComputer();
        
        firstWordsHashMatchComputer.setAffOrgMatchVoters(createNameFirstWordsHashBucketMatcherVoters());
        
        // matcher
        
        return new AffOrgMatcher(firstWordsHashBucketJoiner, firstWordsHashMatchComputer);
    }
    
    
    /**
     * Creates {@link AffOrgMatchVoter}s for {@link #createNameFirstWordsHashBucketMatcher()} 
     */
    public static ImmutableList<AffOrgMatchVoter> createNameFirstWordsHashBucketMatcherVoters() {
        
        CommonWordsVoter commonOrgNameWordsVoter = new CommonWordsVoter(ImmutableList.of(',', ';'), 2, 0.71, WITH_REGARD_TO_ORG_WORDS);
        commonOrgNameWordsVoter.setCommonSimilarWordCalculator(new CommonSimilarWordCalculator(0.9));
        
        CommonWordsVoter commonAffNameWordsVoter = new CommonWordsVoter(ImmutableList.of(',', ';'), 2, 0.81, WITH_REGARD_TO_AFF_WORDS);
        commonAffNameWordsVoter.setCommonSimilarWordCalculator(new CommonSimilarWordCalculator(0.9));
        
        
        CompositeMatchVoter commonAffOrgNameWordsVoter = new CompositeMatchVoter(ImmutableList.of(new CountryCodeLooseMatchVoter(), commonOrgNameWordsVoter, commonAffNameWordsVoter));
        commonAffOrgNameWordsVoter.setMatchStrength(0.859f);
        
        return ImmutableList.of(
                createNameCountryStrictMatchVoter(0.981f, new GetOrgNameFunction()),
                createNameStrictCountryLooseMatchVoter(0.966f, new GetOrgNameFunction()),
                createSectionedNameStrictCountryLooseMatchVoter(0.930f, new GetOrgNameFunction()),
                createSectionedNameLevenshteinCountryLooseMatchVoter(0.922f, new GetOrgNameFunction()),
                createSectionedNameStrictCountryLooseMatchVoter(0.882f, new GetOrgShortNameFunction()),
                commonAffOrgNameWordsVoter);
    }
    
    
    
    
    //------------------------ PRIVATE --------------------------
    
    

    private static AffOrgHashBucketJoiner createFirstWordsAffOrgHashBucketJoiner(Function<AffMatchOrganization, List<String>> getOrgNamesFunction) {
     
        AffOrgHashBucketJoiner firstWordsHashBucketJoiner = new AffOrgHashBucketJoiner();
    
        OrganizationNameBucketHasher orgNameBucketHasher = new OrganizationNameBucketHasher();
        orgNameBucketHasher.setGetOrgNamesFunction(getOrgNamesFunction);
        
        firstWordsHashBucketJoiner.setOrganizationBucketHasher(orgNameBucketHasher);
        
        return firstWordsHashBucketJoiner;
    
    }
}
