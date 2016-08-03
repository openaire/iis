package eu.dnetlib.iis.wf.affmatching.match;

import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.assertCompositeVoter;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.assertInternalMainSectionBucketHasher;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.assertNameStrictWithCharFilteringMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.assertSectionedNameLevenshteinMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.assertSectionedNameStrictMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.getInternalVoter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.powermock.reflect.Whitebox.getInternalState;

import java.util.List;
import java.util.function.Function;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgHashBucketJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.AffiliationOrgNameBucketHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.BucketHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.MainSectionBucketHasher.FallbackSectionPickStrategy;
import eu.dnetlib.iis.wf.affmatching.bucket.OrganizationNameBucketHasher;
import eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CountryCodeLooseMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CountryCodeStrictMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgAlternativeNamesFunction;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgNameFunction;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgShortNameFunction;
import eu.dnetlib.iis.wf.affmatching.match.voter.NameStrictWithCharFilteringMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.SectionedNameLevenshteinMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.SectionedNameStrictMatchVoter;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSection.OrgSectionType;

/**
* @author Łukasz Dumiszewski
*/

public class MainSectionHashBucketMatcherFactoryTest {

    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void createNameMainSectionHashBucketMatcher() {
        
        // execute
        
        AffOrgMatcher matcher = MainSectionHashBucketMatcherFactory.createNameMainSectionHashBucketMatcher();
        
        
        // assert
        
        assertMainSectionHashBucketMatcher(matcher, GetOrgNameFunction.class);
        
        
        AffOrgMatchComputer computer = getInternalState(matcher, AffOrgMatchComputer.class);
        
        List<AffOrgMatchVoter> voters = getInternalState(computer, "affOrgMatchVoters");
        
        assertNameMainSectionHashBucketMatcherVoters(voters);
        
    }


    @Test
    public void createNameMainSectionHashBucketMatcherVoters() {
        
        // execute
        
        List<AffOrgMatchVoter> voters = MainSectionHashBucketMatcherFactory.createNameMainSectionHashBucketMatcherVoters();
        
        // assert
        
        assertNameMainSectionHashBucketMatcherVoters(voters);
        
        
    }

    
    @Test
    public void createAlternativeNameMainSectionHashBucketMatcher() {
        
        // execute
        
        AffOrgMatcher matcher = MainSectionHashBucketMatcherFactory.createAlternativeNameMainSectionHashBucketMatcher();
        
        
        // assert
        
        assertMainSectionHashBucketMatcher(matcher, GetOrgAlternativeNamesFunction.class);
        
        
        AffOrgMatchComputer computer = getInternalState(matcher, AffOrgMatchComputer.class);
        
        List<AffOrgMatchVoter> voters = getInternalState(computer, "affOrgMatchVoters");
        
        assertAlternativeNameMainSectionHashBucketMatcherVoters(voters);
        
    }


    @Test
    public void createAlternativeNameMainSectionHashBucketMatcherVoters() {
        
        // execute
        
        List<AffOrgMatchVoter> voters = MainSectionHashBucketMatcherFactory.createAlternativeNameMainSectionHashBucketMatcherVoters();
        
        // assert
        
        assertAlternativeNameMainSectionHashBucketMatcherVoters(voters);
        
        
    }

    
    @Test
    public void createShortNameMainSectionHashBucketMatcher() {
        
        // execute
        
        AffOrgMatcher matcher = MainSectionHashBucketMatcherFactory.createShortNameMainSectionHashBucketMatcher();
        
        
        // assert
        
        assertMainSectionHashBucketMatcher(matcher, GetOrgShortNameFunction.class);
        
        
        AffOrgMatchComputer computer = getInternalState(matcher, AffOrgMatchComputer.class);
        
        List<AffOrgMatchVoter> voters = getInternalState(computer, "affOrgMatchVoters");
        
        assertShortNameMainSectionHashBucketMatcherVoters(voters);
        
    }


    @Test
    public void createShortNameMainSectionHashBucketMatcherVoters() {
        
        // execute
        
        List<AffOrgMatchVoter> voters = MainSectionHashBucketMatcherFactory.createShortNameMainSectionHashBucketMatcherVoters();
        
        // assert
        
        assertShortNameMainSectionHashBucketMatcherVoters(voters);
        
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    
    
    private void assertMainSectionHashBucketMatcher(AffOrgMatcher matcher, Class<? extends Function<AffMatchOrganization, List<String>>> expectedGetOrgNamesFunctionClass) {
        
        AffOrgJoiner joiner = getInternalState(matcher, AffOrgJoiner.class);
        assertTrue(joiner instanceof AffOrgHashBucketJoiner);
        
        
        BucketHasher<AffMatchAffiliation> affHasher = getInternalState(joiner, "affiliationBucketHasher");
        assertTrue(affHasher instanceof AffiliationOrgNameBucketHasher);
        
        assertInternalMainSectionBucketHasher(affHasher, OrgSectionType.UNIVERSITY, FallbackSectionPickStrategy.LAST_SECTION);
        
        
        BucketHasher<AffMatchOrganization> orgHasher = getInternalState(joiner, "organizationBucketHasher");
        assertTrue(orgHasher instanceof OrganizationNameBucketHasher);
        
        Function<AffMatchOrganization, List<String>> getOrgNamesFunction = getInternalState(orgHasher, "getOrgNamesFunction");
        assertEquals(expectedGetOrgNamesFunctionClass, getOrgNamesFunction.getClass());
        
        assertInternalMainSectionBucketHasher(orgHasher, OrgSectionType.UNIVERSITY, FallbackSectionPickStrategy.FIRST_SECTION);
    }
    
    private void assertNameMainSectionHashBucketMatcherVoters(List<AffOrgMatchVoter> voters) {
        assertEquals(5, voters.size());
       
        assertCompositeVoter(voters.get(0), CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(0), 1), ImmutableList.of(',', ';'), GetOrgNameFunction.class);
        
        assertCompositeVoter(voters.get(1), CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(1), 1), ImmutableList.of(',', ';'), GetOrgNameFunction.class);
       
        assertCompositeVoter(voters.get(2), CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
        assertSectionedNameStrictMatchVoter(getInternalVoter(voters.get(2), 1), GetOrgNameFunction.class);
       
        assertCompositeVoter(voters.get(3), CountryCodeLooseMatchVoter.class, SectionedNameLevenshteinMatchVoter.class);
        assertSectionedNameLevenshteinMatchVoter(getInternalVoter(voters.get(3), 1), 0.9f, GetOrgNameFunction.class);
       
        assertCompositeVoter(voters.get(4), CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
        assertSectionedNameStrictMatchVoter(getInternalVoter(voters.get(4), 1), GetOrgShortNameFunction.class);
    }
   
    private void assertAlternativeNameMainSectionHashBucketMatcherVoters(List<AffOrgMatchVoter> voters) {
       
        assertEquals(4, voters.size());
       
        assertCompositeVoter(voters.get(0), CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(0), 1), ImmutableList.of(',', ';'), GetOrgAlternativeNamesFunction.class);
       
        assertCompositeVoter(voters.get(1), CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(1), 1), ImmutableList.of(',', ';'), GetOrgAlternativeNamesFunction.class);
       
        assertCompositeVoter(voters.get(2), CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
        assertSectionedNameStrictMatchVoter(getInternalVoter(voters.get(2), 1), GetOrgAlternativeNamesFunction.class);
       
        assertCompositeVoter(voters.get(3), CountryCodeLooseMatchVoter.class, SectionedNameLevenshteinMatchVoter.class);
        assertSectionedNameLevenshteinMatchVoter(getInternalVoter(voters.get(3), 1), 0.9f, GetOrgAlternativeNamesFunction.class);
       
    } 
   
    private void assertShortNameMainSectionHashBucketMatcherVoters(List<AffOrgMatchVoter> voters) {
       
        assertEquals(2, voters.size());
       
        assertCompositeVoter(voters.get(0), CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(0), 1), ImmutableList.of(',', ';'), GetOrgShortNameFunction.class);
       
        assertCompositeVoter(voters.get(1), CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(1), 1), ImmutableList.of(',', ';'), GetOrgShortNameFunction.class);
       
    }
    
}
