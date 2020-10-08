package eu.dnetlib.iis.wf.affmatching.match;

import com.google.common.collect.ImmutableList;
import eu.dnetlib.iis.wf.affmatching.bucket.*;
import eu.dnetlib.iis.wf.affmatching.bucket.MainSectionBucketHasher.FallbackSectionPickStrategy;
import eu.dnetlib.iis.wf.affmatching.match.voter.*;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSection.OrgSectionType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Function;

import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.powermock.reflect.Whitebox.getInternalState;

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
