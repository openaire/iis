package eu.dnetlib.iis.wf.affmatching.match.voter;

import com.google.common.collect.ImmutableList;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.powermock.reflect.Whitebox;

import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author madryk
 */
@ExtendWith(MockitoExtension.class)
public class AffOrgMatchVotersFactoryTest {

    private static final double PRECISION = 1e10-6;
    
    private float matchStrength = 0.75f;
    
    @Mock
    private Function<AffMatchOrganization, List<String>> getOrgNamesFunction;
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void createNameCountryStrictMatchVoter() {
        
        // execute
        
        AffOrgMatchVoter voter = AffOrgMatchVotersFactory.createNameCountryStrictMatchVoter(matchStrength, getOrgNamesFunction);
        
        // assert
        
        assertEquals(matchStrength, voter.getMatchStrength(), PRECISION);
        assertTrue(voter instanceof CompositeMatchVoter);
        assertInternalVotersCount(voter, 2);
        
        assertTrue(getInternalVoter(voter, 0) instanceof CountryCodeStrictMatchVoter);
        assertOrgNameVoter(getInternalVoter(voter, 1), NameStrictWithCharFilteringMatchVoter.class, getOrgNamesFunction);

        
        List<Character> charsToFilter = Whitebox.getInternalState(getInternalVoter(voter, 1), "charsToFilter");
        assertEquals(ImmutableList.of(',', ';'), charsToFilter);
    }

    @Test
    public void createNameStrictCountryLooseMatchVoter() {
        
        // execute
        
        AffOrgMatchVoter voter = AffOrgMatchVotersFactory.createNameStrictCountryLooseMatchVoter(matchStrength, getOrgNamesFunction);
        
        // assert
        
        assertEquals(matchStrength, voter.getMatchStrength(), PRECISION);
        assertTrue(voter instanceof CompositeMatchVoter);
        assertInternalVotersCount(voter, 2);
        
        assertTrue(getInternalVoter(voter, 0) instanceof CountryCodeLooseMatchVoter);
        assertOrgNameVoter(getInternalVoter(voter, 1), NameStrictWithCharFilteringMatchVoter.class, getOrgNamesFunction);

        
        List<Character> charsToFilter = Whitebox.getInternalState(getInternalVoter(voter, 1), "charsToFilter");
        assertEquals(ImmutableList.of(',', ';'), charsToFilter);
    }
    
    @Test
    public void createSectionedNameStrictCountryLooseMatchVoter() {
        
        // execute
        
        AffOrgMatchVoter voter = AffOrgMatchVotersFactory.createSectionedNameStrictCountryLooseMatchVoter(matchStrength, getOrgNamesFunction);
        
        // assert
        
        assertEquals(matchStrength, voter.getMatchStrength(), PRECISION);
        assertTrue(voter instanceof CompositeMatchVoter);
        assertInternalVotersCount(voter, 2);
        
        assertTrue(getInternalVoter(voter, 0) instanceof CountryCodeLooseMatchVoter);
        assertOrgNameVoter(getInternalVoter(voter, 1), SectionedNameStrictMatchVoter.class, getOrgNamesFunction);


    }
    
    
    @Test
    public void createSectionedNameLevenshteinCountryLooseMatchVoter() {
        
        // execute
        
        AffOrgMatchVoter voter = AffOrgMatchVotersFactory.createSectionedNameLevenshteinCountryLooseMatchVoter(matchStrength, getOrgNamesFunction);
        
        // assert
        
        assertEquals(matchStrength, voter.getMatchStrength(), PRECISION);
        assertTrue(voter instanceof CompositeMatchVoter);
        assertInternalVotersCount(voter, 2);
        
        assertTrue(getInternalVoter(voter, 0) instanceof CountryCodeLooseMatchVoter);
        assertOrgNameVoter(getInternalVoter(voter, 1), SectionedNameLevenshteinMatchVoter.class, getOrgNamesFunction);

        assertEquals(0.9, Whitebox.getInternalState(getInternalVoter(voter, 1), "minSimilarity"), PRECISION);
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void assertInternalVotersCount(AffOrgMatchVoter voter, int expectedCount) {
        
        List<AffOrgMatchVoter> internalVoters = Whitebox.getInternalState(voter, "voters");
        
        assertEquals(expectedCount, internalVoters.size());
    }
    
    
    private AffOrgMatchVoter getInternalVoter(AffOrgMatchVoter voter, int position) {
        
        List<AffOrgMatchVoter> internalVoters = Whitebox.getInternalState(voter, "voters");
        
        return internalVoters.get(position);
    }
    
   
    private void assertOrgNameVoter(AffOrgMatchVoter voter1, Class<? extends AffOrgMatchVoter> expectedVoterClass,Function<AffMatchOrganization, List<String>> expectedGetOrgNamesFunction) {

        assertEquals(voter1.getClass(), expectedVoterClass);

        assertEquals(Whitebox.getInternalState(voter1, "getOrgNamesFunction"), getOrgNamesFunction);
    }
}
