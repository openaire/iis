package eu.dnetlib.iis.wf.affmatching.match.voter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.ImmutableList;

/**
 * @author madryk
 */
public class AffOrgMatchVotersFactoryTest {

    private static final double PRECISION = 1e10-6;
    
    
    private float matchStrength = 0.75f;
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void createNameCountryStrictMatchVoter() {
        
        // execute
        
        AffOrgMatchVoter voter = AffOrgMatchVotersFactory.createNameCountryStrictMatchVoter(matchStrength);
        
        // assert
        
        assertEquals(matchStrength, voter.getMatchStrength(), PRECISION);
        assertTrue(voter instanceof CompositeMatchVoter);
        assertInternalVotersCount(voter, 2);
        
        assertTrue(getInternalVoter(voter, 0) instanceof CountryCodeStrictMatchVoter);
        AffOrgMatchVoter voter1 = getInternalVoter(voter, 1);
        assertTrue(voter1 instanceof NameStrictWithCharFilteringMatchVoter);
        assertTrue(Whitebox.getInternalState(voter1, "getOrgNamesFunction") instanceof GetOrgNameFunction);

        
        List<Character> charsToFilter = Whitebox.getInternalState(getInternalVoter(voter, 1), "charsToFilter");
        assertEquals(ImmutableList.of(',', ';'), charsToFilter);
    }
    
    @Test
    public void createNameStrictCountryLooseMatchVoter() {
        
        // execute
        
        AffOrgMatchVoter voter = AffOrgMatchVotersFactory.createNameStrictCountryLooseMatchVoter(matchStrength);
        
        // assert
        
        assertEquals(matchStrength, voter.getMatchStrength(), PRECISION);
        assertTrue(voter instanceof CompositeMatchVoter);
        assertInternalVotersCount(voter, 2);
        
        assertTrue(getInternalVoter(voter, 0) instanceof CountryCodeLooseMatchVoter);
        AffOrgMatchVoter voter1 = getInternalVoter(voter, 1);
        assertTrue(voter1 instanceof NameStrictWithCharFilteringMatchVoter);
        assertTrue(Whitebox.getInternalState(voter1, "getOrgNamesFunction") instanceof GetOrgNameFunction);

        
        List<Character> charsToFilter = Whitebox.getInternalState(getInternalVoter(voter, 1), "charsToFilter");
        assertEquals(ImmutableList.of(',', ';'), charsToFilter);
    }
    
    @Test
    public void createSectionedNameStrictCountryLooseMatchVoter() {
        
        // execute
        
        AffOrgMatchVoter voter = AffOrgMatchVotersFactory.createSectionedNameStrictCountryLooseMatchVoter(matchStrength);
        
        // assert
        
        assertEquals(matchStrength, voter.getMatchStrength(), PRECISION);
        assertTrue(voter instanceof CompositeMatchVoter);
        assertInternalVotersCount(voter, 2);
        
        assertTrue(getInternalVoter(voter, 0) instanceof CountryCodeLooseMatchVoter);
        AffOrgMatchVoter voter1 = getInternalVoter(voter, 1);
        assertTrue(voter1 instanceof SectionedNameStrictMatchVoter);
        assertTrue(Whitebox.getInternalState(voter1, "getOrgNamesFunction") instanceof GetOrgNameFunction);

    }
    
    @Test
    public void createSectionedShortNameStrictCountryLooseMatchVoter() {
        
        // execute
        
        AffOrgMatchVoter voter = AffOrgMatchVotersFactory.createSectionedShortNameStrictCountryLooseMatchVoter(matchStrength);
        
        // assert
        
        assertEquals(matchStrength, voter.getMatchStrength(), PRECISION);
        assertTrue(voter instanceof CompositeMatchVoter);
        assertInternalVotersCount(voter, 2);
        
        assertTrue(getInternalVoter(voter, 0) instanceof CountryCodeLooseMatchVoter);
        AffOrgMatchVoter voter1 = getInternalVoter(voter, 1);
        assertTrue(voter1 instanceof SectionedNameStrictMatchVoter);
        assertTrue(Whitebox.getInternalState(voter1, "getOrgNamesFunction") instanceof GetOrgShortNameFunction);
    }
    
    @Test
    public void createSectionedNameLevenshteinCountryLooseMatchVoter() {
        
        // execute
        
        AffOrgMatchVoter voter = AffOrgMatchVotersFactory.createSectionedNameLevenshteinCountryLooseMatchVoter(matchStrength);
        
        // assert
        
        assertEquals(matchStrength, voter.getMatchStrength(), PRECISION);
        assertTrue(voter instanceof CompositeMatchVoter);
        assertInternalVotersCount(voter, 2);
        
        assertTrue(getInternalVoter(voter, 0) instanceof CountryCodeLooseMatchVoter);
        AffOrgMatchVoter voter1 = getInternalVoter(voter, 1);
        assertTrue(voter1 instanceof SectionedNameLevenshteinMatchVoter);
        assertTrue(Whitebox.getInternalState(voter1, "getOrgNamesFunction") instanceof GetOrgNameFunction);

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
}
