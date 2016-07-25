package eu.dnetlib.iis.wf.affmatching.match.voter;

import com.google.common.collect.ImmutableList;

/**
 * @author madryk
 */
public class AffOrgMatchVotersFactory {
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    private AffOrgMatchVotersFactory() { }
    
    
    //------------------------ LOGIC --------------------------
    
    public static AffOrgMatchVoter createNameCountryStrictMatchVoter(float matchStrength) {
        
        CompositeMatchVoter voter = new CompositeMatchVoter(ImmutableList.of(
                new CountryCodeStrictMatchVoter(), 
                new NameStrictWithCharFilteringMatchVoter(ImmutableList.of(',', ';'))));
        
        voter.setMatchStrength(matchStrength);
        
        return voter;
        
    }
    
    public static AffOrgMatchVoter createNameStrictCountryLooseMatchVoter(float matchStrength) {
        
        CompositeMatchVoter voter = new CompositeMatchVoter(ImmutableList.of(
                new CountryCodeLooseMatchVoter(), 
                new NameStrictWithCharFilteringMatchVoter(ImmutableList.of(',', ';'))));
        
        voter.setMatchStrength(matchStrength);
        
        return voter;
        
    }
    
    public static AffOrgMatchVoter createSectionedNameStrictCountryLooseMatchVoter(float matchStrength) {
        
        CompositeMatchVoter voter = new CompositeMatchVoter(ImmutableList.of(
                new CountryCodeLooseMatchVoter(), 
                new SectionedNameStrictMatchVoter()));
        
        voter.setMatchStrength(matchStrength);
        
        return voter;
        
    }
    
    public static AffOrgMatchVoter createSectionedShortNameStrictCountryLooseMatchVoter(float matchStrength) {
        
        SectionedNameStrictMatchVoter sectionedShortNameStrictMatchVoter = new SectionedNameStrictMatchVoter();
        sectionedShortNameStrictMatchVoter.setGetOrgNamesFunction(new GetOrgShortNameFunction());
        
        CompositeMatchVoter voter = new CompositeMatchVoter(ImmutableList.of(
                new CountryCodeLooseMatchVoter(), 
                sectionedShortNameStrictMatchVoter));
        
        voter.setMatchStrength(matchStrength);
        
        return voter;
    }
    
    public static AffOrgMatchVoter createSectionedNameLevenshteinCountryLooseMatchVoter(float matchStrength) {
        
        CompositeMatchVoter voter = new CompositeMatchVoter(ImmutableList.of(
                new CountryCodeLooseMatchVoter(), 
                new SectionedNameLevenshteinMatchVoter(0.9)));
        
        voter.setMatchStrength(matchStrength);
        
        return voter;
    }
    
}
