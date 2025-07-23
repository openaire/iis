package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * @author madryk
 */
public final class AffOrgMatchVotersFactory {
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    private AffOrgMatchVotersFactory() { }
    
    
    //------------------------ LOGIC --------------------------
    
    public static AffOrgMatchVoter createNameCountryStrictMatchVoter(float matchStrength, Function<AffMatchOrganization, List<String>> getOrgNamesFunction) {
       
        NameStrictWithCharFilteringMatchVoter orgNameVoter = new NameStrictWithCharFilteringMatchVoter(ImmutableList.of(',', ';'));
        orgNameVoter.setGetOrgNamesFunction(getOrgNamesFunction);
       
        CompositeMatchVoter voter = new CompositeMatchVoter(ImmutableList.of(new CountryCodeStrictMatchVoter(), orgNameVoter));
        
        voter.setMatchStrength(matchStrength);
        
        return voter;
        
    }
  
    
    public static AffOrgMatchVoter createNameStrictCountryLooseMatchVoter(float matchStrength, Function<AffMatchOrganization, List<String>> getOrgNamesFunction) {
        
        NameStrictWithCharFilteringMatchVoter orgNameVoter = new NameStrictWithCharFilteringMatchVoter(ImmutableList.of(',', ';'));
        orgNameVoter.setGetOrgNamesFunction(getOrgNamesFunction);
        
        CompositeMatchVoter voter = new CompositeMatchVoter(ImmutableList.of(new CountryCodeLooseMatchVoter(), orgNameVoter));
        
        voter.setMatchStrength(matchStrength);
        
        return voter;
        
    }
    
    public static AffOrgMatchVoter createSectionedNameStrictCountryLooseMatchVoter(float matchStrength, Function<AffMatchOrganization, List<String>> getOrgNamesFunction) {
        
        SectionedNameStrictMatchVoter orgNameVoter = new SectionedNameStrictMatchVoter();
        orgNameVoter.setGetOrgNamesFunction(getOrgNamesFunction);
        
        CompositeMatchVoter voter = new CompositeMatchVoter(ImmutableList.of(new CountryCodeLooseMatchVoter(), orgNameVoter));
        
        voter.setMatchStrength(matchStrength);
        
        return voter;
        
    }
    
    public static AffOrgMatchVoter createSectionedNameLevenshteinCountryLooseMatchVoter(float matchStrength, Function<AffMatchOrganization, List<String>> getOrgNamesFunction) {
        
        SectionedNameLevenshteinMatchVoter orgNameVoter = new SectionedNameLevenshteinMatchVoter(0.91);
        orgNameVoter.setGetOrgNamesFunction(getOrgNamesFunction);
        
        CompositeMatchVoter voter = new CompositeMatchVoter(ImmutableList.of(new CountryCodeLooseMatchVoter(), orgNameVoter));
        
        voter.setMatchStrength(matchStrength);
        
        return voter;
    }
    
}
