package eu.dnetlib.iis.wf.affmatching.match.voter;

import com.google.common.collect.Lists;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author madryk
 */
@ExtendWith(MockitoExtension.class)
public class SectionedNameLevenshteinMatchVoterTest {

    @InjectMocks
    private SectionedNameLevenshteinMatchVoter voter = new SectionedNameLevenshteinMatchVoter(0.9);
    
    @Mock
    private Function<AffMatchOrganization, List<String>> getOrgNamesFunction;
    
    
    private AffMatchAffiliation affiliation = new AffMatchAffiliation("DOC1", 1);
    
    private AffMatchOrganization organization = new AffMatchOrganization("ORG1");
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void voteMatch_match_exact_single_sectioned() {

        // given
        affiliation.setOrganizationName("philipps universitat marburg");
        resetOrgNames("philipps universitat marburg");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    
    @Test
    public void voteMatch_match_exact_single_sectioned___many_orgs() {

        // given
        affiliation.setOrganizationName("philipps universitat marburg");
        resetOrgNames("some other inst", "philipps universitat marburg");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_similar_single_sectioned() {

        // given
        affiliation.setOrganizationName("philipps university marburg");
        resetOrgNames("philipps universitat marburg");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_similar_single_sectioned___many_orgs() {

        // given
        affiliation.setOrganizationName("philipps university marburg");
        resetOrgNames("filips polska", "philipps universitat marburg", "university of warsaw");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    
    @Test
    public void voteMatch_match_exact_one_section() {

        // given
        affiliation.setOrganizationName("center for human genetics, philipps universitat marburg");
        resetOrgNames("philipps universitat marburg");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_similar_one_section() {

        // given
        affiliation.setOrganizationName("center for human genetics, philipps university marburg");
        resetOrgNames("philipps universitat marburg");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_similar_multi_sections() {

        // given
        affiliation.setOrganizationName("center for human genetics, philipps university marburg");
        resetOrgNames("philipps universitat marburg, center for human genetic");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_similar_multi_sections___many_orgs() {

        // given
        affiliation.setOrganizationName("center for human genetics, philipps university marburg");
        resetOrgNames("phillips univesity, centre for biology", "philipps universitat marburg, center for human genetic", "yet another university");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_dont_match_not_similar_single_sectioned() {

        // given
        affiliation.setOrganizationName("philips university marburg");
        resetOrgNames("philipps universitat marburg"); // levenshtein distance: 3; similarity: 0.89
        
        // execute & assert
        assertFalse(voter.voteMatch(affiliation, organization));
        
    }
    
    
    @Test
    public void voteMatch_dont_match_not_similar_single_sectioned__many_orgs() {

        // given
        affiliation.setOrganizationName("philips university marburg");
        resetOrgNames("university of berlin", "philipps universitat marburg"); // levenshtein distance: 3; similarity: 0.89
        
        // execute & assert
        assertFalse(voter.voteMatch(affiliation, organization));
        
    }
    
    
    @Test
    public void voteMatch_dont_match_not_similar_multi_sections() {

        // given
        affiliation.setOrganizationName("center for human genetics, philipps university marburg");
        resetOrgNames("philipps universitat marburg, center human genetics");
        // levenshtein distance: 2; similarity: 0.92 (university section)
        // levenshtein distance: 4; similarity: 0.81 (center section)
        
        // execute & assert
        assertFalse(voter.voteMatch(affiliation, organization));
        
    }
    
    //------------------------ PRIVATE --------------------------
    
    private void resetOrgNames(String... orgNames) {
        Mockito.when(getOrgNamesFunction.apply(organization)).thenReturn(Lists.newArrayList(orgNames));
    }
}
