package eu.dnetlib.iis.wf.affmatching.match.voter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.function.Function;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class SectionedNameStrictMatchVoterTest {

    @InjectMocks
    private SectionedNameStrictMatchVoter voter = new SectionedNameStrictMatchVoter();
    
    @Mock
    private Function<AffMatchOrganization, List<String>> getOrgNamesFunction;
    
    
    private AffMatchAffiliation affiliation = new AffMatchAffiliation("DOC1", 1);
    
    private AffMatchOrganization organization = new AffMatchOrganization("ORG1");
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void voteMatch_match_single_sectioned() {

        // given
        affiliation.setOrganizationName("university of alberta");
        resetOrgNames("university of alberta");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_single_sectioned___many_orgs() {

        // given
        affiliation.setOrganizationName("university of alberta");
        resetOrgNames("university of warsaw", "university of alberta", "some foolish institution");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    
    @Test
    public void voteMatch_match_multi_sectioned_aff_to_single_sectioned_org() {
        
        // given
        affiliation.setOrganizationName("department of pediatrics, faculty of medicine and dentistry, university of alberta");
        resetOrgNames("university of alberta");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    
    @Test
    public void voteMatch_match_multi_sectioned_aff_to_single_sectioned_org___many_orgs() {
        
        // given
        affiliation.setOrganizationName("department of pediatrics, faculty of medicine and dentistry, university of alberta");
        resetOrgNames("icm uw", "university of alberta");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
       
    @Test
    public void voteMatch_match_multi_sectioned_aff_to_multi_sectioned_org() {

        // given
        affiliation.setOrganizationName("department of pediatrics, faculty of medicine and dentistry, university of alberta");
        resetOrgNames("department of pediatrics, university of alberta");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_multi_sectioned_aff_to_multi_sectioned_org___many_orgs() {

        // given
        affiliation.setOrganizationName("department of pediatrics, faculty of medicine and dentistry, university of alberta");
        resetOrgNames("university of bicycles, toronto", "department of pediatrics, university of alberta", "department of pediatrics, university of michigan");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_multi_sectioned_aff_to_inverted_multi_sectioned_org() {

        // given
        affiliation.setOrganizationName("department of pediatrics, faculty of medicine and dentistry, university of alberta");
        resetOrgNames("university of alberta, department of pediatrics");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_multi_sectioned_aff_to_multi_sectioned_org_SEMICOLONS() {

        // given
        affiliation.setOrganizationName("department of pediatrics; faculty of medicine and dentistry; university of alberta");
        resetOrgNames("department of pediatrics, university of alberta");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_aff_with_restricted_section() {

        // given
        affiliation.setOrganizationName("medshape, inc");
        resetOrgNames("medshape");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_org_with_restricted_section() {

        // given
        affiliation.setOrganizationName("medshape");
        resetOrgNames("medshape, inc");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_dont_match_different_single_sectioned() {

        // given
        affiliation.setOrganizationName("university of ontario");
        resetOrgNames("university of alberta");
        
        // execute & assert
        assertFalse(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_dont_match_different_single_sectioned___many_orgs() {

        // given
        affiliation.setOrganizationName("university of ontario");
        resetOrgNames("university of warsaw", "university of alberta");
        
        // execute & assert
        assertFalse(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_dont_match_org_with_different_one_section() {

        // given
        affiliation.setOrganizationName("department of pediatrics, faculty of medicine and dentistry, university of alberta");
        resetOrgNames("department of psychology, university of alberta");
        
        // execute & assert
        assertFalse(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_dont_match_only_restricted_section() {

        // given
        affiliation.setOrganizationName("inc");
        resetOrgNames("inc");
        
        // execute & assert
        assertFalse(voter.voteMatch(affiliation, organization));
        
    }

    
    //------------------------ PRIVATE --------------------------
    
    private void resetOrgNames(String... orgNames) {
        Mockito.when(getOrgNamesFunction.apply(organization)).thenReturn(Lists.newArrayList(orgNames));
    }

}
