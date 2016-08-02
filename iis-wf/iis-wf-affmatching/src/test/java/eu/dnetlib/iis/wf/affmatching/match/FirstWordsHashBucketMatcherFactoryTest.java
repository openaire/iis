package eu.dnetlib.iis.wf.affmatching.match;

import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.assertCommonWordsVoter;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.assertCompositeVoter;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.assertInternalStringPartFirstLettersHasher;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.assertNameStrictWithCharFilteringMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.assertSectionedNameLevenshteinMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.assertSectionedNameStrictMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.getInternalVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.CommonWordsVoter.RatioRelation.WITH_REGARD_TO_AFF_WORDS;
import static eu.dnetlib.iis.wf.affmatching.match.voter.CommonWordsVoter.RatioRelation.WITH_REGARD_TO_ORG_WORDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.powermock.reflect.Whitebox.getInternalState;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgHashBucketJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.AffiliationOrgNameBucketHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.BucketHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.OrganizationNameBucketHasher;
import eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CommonWordsVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CountryCodeLooseMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CountryCodeStrictMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgNameFunction;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgShortNameFunction;
import eu.dnetlib.iis.wf.affmatching.match.voter.NameStrictWithCharFilteringMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.SectionedNameLevenshteinMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.SectionedNameStrictMatchVoter;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
* @author Łukasz Dumiszewski
*/

public class FirstWordsHashBucketMatcherFactoryTest {
    
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void createNameFirstWordsHashBucketMatcher() {
        
        // execute
        
        AffOrgMatcher matcher = FirstWordsHashBucketMatcherFactory.createNameFirstWordsHashBucketMatcher();
        
        
        // assert
        
        AffOrgJoiner joiner = getInternalState(matcher, AffOrgJoiner.class);
        assertTrue(joiner instanceof AffOrgHashBucketJoiner);
        
        
        BucketHasher<AffMatchAffiliation> affHasher = getInternalState(joiner, "affiliationBucketHasher");
        assertTrue(affHasher instanceof AffiliationOrgNameBucketHasher);
        
        assertInternalStringPartFirstLettersHasher(affHasher, 2, 2);
        
        
        BucketHasher<AffMatchOrganization> orgHasher = getInternalState(joiner, "organizationBucketHasher");
        assertTrue(orgHasher instanceof OrganizationNameBucketHasher);
        
        assertInternalStringPartFirstLettersHasher(orgHasher, 2, 2);
        
        
        
        AffOrgMatchComputer computer = getInternalState(matcher, AffOrgMatchComputer.class);
        
        List<AffOrgMatchVoter> voters = getInternalState(computer, "affOrgMatchVoters");
        
        assertNameFirstWordsHashBucketMatcherVoters(voters);
        
    }
    
    
    @Test
    public void createNameFirstWordsHashBucketMatcherVoters() {
        
        // execute
        
        List<AffOrgMatchVoter> voters = FirstWordsHashBucketMatcherFactory.createNameFirstWordsHashBucketMatcherVoters();
        
        
        // assert
        
        assertNameFirstWordsHashBucketMatcherVoters(voters);
    }


    
    //------------------------ PRIVATE --------------------------
    
 

   
   
    private void assertNameFirstWordsHashBucketMatcherVoters(List<AffOrgMatchVoter> voters) {
        
        assertEquals(6, voters.size());
       
        assertCompositeVoter(voters.get(0), CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(0), 1), ImmutableList.of(',', ';'), GetOrgNameFunction.class);
       
        assertCompositeVoter(voters.get(1), CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(1), 1), ImmutableList.of(',', ';'), GetOrgNameFunction.class);
       
        assertCompositeVoter(voters.get(2), CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
       
        assertCompositeVoter(voters.get(3), CountryCodeLooseMatchVoter.class, SectionedNameLevenshteinMatchVoter.class);
        assertSectionedNameLevenshteinMatchVoter(getInternalVoter(voters.get(3), 1), 0.9f, GetOrgNameFunction.class);
       
        assertCompositeVoter(voters.get(4), CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
        assertSectionedNameStrictMatchVoter(getInternalVoter(voters.get(4), 1), GetOrgShortNameFunction.class);
       
        assertCompositeVoter(voters.get(5), CountryCodeLooseMatchVoter.class, CommonWordsVoter.class, CommonWordsVoter.class);
        assertCommonWordsVoter(getInternalVoter(voters.get(5), 1), ImmutableList.of(',', ';'), 0.7f, WITH_REGARD_TO_ORG_WORDS, 0.9f, 2, GetOrgNameFunction.class);
        assertCommonWordsVoter(getInternalVoter(voters.get(5), 2), ImmutableList.of(',', ';'), 0.8f, WITH_REGARD_TO_AFF_WORDS, 0.9f, 2, GetOrgNameFunction.class);
    }
   
}
