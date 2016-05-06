package eu.dnetlib.iis.wf.affmatching.match.voter;

import com.google.common.collect.ImmutableList;

/**
 * @author madryk
 */
public class AffOrgMatchVotersFactory {
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    private AffOrgMatchVotersFactory() { }
    
    
    //------------------------ LOGIC --------------------------
    
    public static AffOrgMatchVoter createNameCountryStrictMatchVoter() {
        
        return new CompositeMatchVoter(ImmutableList.of(
                new CountryCodeStrictMatchVoter(), 
                new NameStrictWithCharFilteringMatchVoter(ImmutableList.of(',', ';'))));
        
    }
    
    public static AffOrgMatchVoter createNameStrictCountryLooseMatchVoter() {
        
        return new CompositeMatchVoter(ImmutableList.of(
                new CountryCodeLooseMatchVoter(), 
                new NameStrictWithCharFilteringMatchVoter(ImmutableList.of(',', ';'))));
        
    }
    
    public static AffOrgMatchVoter createSectionedNameStrictCountryLooseMatchVoter() {
        
        return new CompositeMatchVoter(ImmutableList.of(
                new CountryCodeLooseMatchVoter(), 
                new SectionedNameStrictMatchVoter()));
        
    }
    
    public static AffOrgMatchVoter createSectionedShortNameStrictCountryLooseMatchVoter() {
        
        return new CompositeMatchVoter(ImmutableList.of(
                new CountryCodeLooseMatchVoter(), 
                new SectionedShortNameStrictMatchVoter()));
        
    }
    
    public static AffOrgMatchVoter createSectionedNameLevenshteinCountryLooseMatchVoter() {
        
        return new CompositeMatchVoter(ImmutableList.of(
                new CountryCodeLooseMatchVoter(), 
                new SectionedNameLevenshteinMatchVoter(0.9)));
        
    }
}
