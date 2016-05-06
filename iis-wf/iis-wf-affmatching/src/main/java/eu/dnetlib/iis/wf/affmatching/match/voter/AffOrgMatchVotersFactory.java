package eu.dnetlib.iis.wf.affmatching.match.voter;

import com.google.common.collect.Lists;

/**
 * @author madryk
 */
public class AffOrgMatchVotersFactory {
    
    
    //------------------------ LOGIC --------------------------
    
    public AffOrgMatchVoter createNameCountryStrictMatchVoter() {
        
        return new CompositeMatchVoter(Lists.newArrayList(
                new CountryCodeStrictMatchVoter(), 
                new NameStrictWithCharFilteringMatchVoter(Lists.newArrayList(',', ';'))));
        
    }
    
    public AffOrgMatchVoter createNameStrictCountryLooseMatchVoter() {
        
        return new CompositeMatchVoter(Lists.newArrayList(
                new CountryCodeLooseMatchVoter(), 
                new NameStrictWithCharFilteringMatchVoter(Lists.newArrayList(',', ';'))));
        
    }
    
    public AffOrgMatchVoter createSectionedNameStrictCountryLooseMatchVoter() {
        
        return new CompositeMatchVoter(Lists.newArrayList(
                new CountryCodeLooseMatchVoter(), 
                new SectionedNameStrictMatchVoter()));
        
    }
    
    public AffOrgMatchVoter createSectionedShortNameStrictCountryLooseMatchVoter() {
        
        return new CompositeMatchVoter(Lists.newArrayList(
                new CountryCodeLooseMatchVoter(), 
                new SectionedShortNameStrictMatchVoter()));
        
    }
    
    public AffOrgMatchVoter createSectionedNameLevensteinCountryLooseMatchVoter() {
        
        return new CompositeMatchVoter(Lists.newArrayList(
                new CountryCodeLooseMatchVoter(), 
                new SectionedNameLevensteinMatchVoter(0.9)));
        
    }
}
