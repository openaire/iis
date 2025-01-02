package eu.dnetlib.iis.wf.affmatching.match;

import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createNameCountryStrictMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createNameStrictCountryLooseMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createSectionedNameLevenshteinCountryLooseMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createSectionedNameStrictCountryLooseMatchVoter;

import java.util.List;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;

import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgHashBucketJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.AffiliationOrgNameBucketHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.MainSectionBucketHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.MainSectionBucketHasher.FallbackSectionPickStrategy;
import eu.dnetlib.iis.wf.affmatching.bucket.OrganizationNameBucketHasher;
import eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgAlternativeNamesFunction;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgNameFunction;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgShortNameFunction;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * A factory of {@link AffOrgMatcher}s that join organizations and affiliations into buckets based on hashes produced from
 * the main sections of organization names. 
 * 
 * @author Łukasz Dumiszewski
*/

public final class MainSectionHashBucketMatcherFactory {
    
    // ---------------------- CONSTRUCTORS ----------------------
    
    private MainSectionHashBucketMatcherFactory() {}
    
    //------------------------ LOGIC --------------------------
    
      
    /**
     * Returns {@link AffOrgMatcher} that uses hashing of affiliations and organizations to create buckets.<br/>
     * Hashes are computed based on main section of {@link AffMatchAffiliation#getOrganizationName()}
     * and {@link AffMatchOrganization#getName()}.
     * 
     * @see MainSectionBucketHasher#hash(String)
     */
    public static AffOrgMatcher createNameMainSectionHashBucketMatcher() {
        
        // joiner
        
        AffOrgHashBucketJoiner mainSectionHashBucketJoiner = createMainSectionAffOrgHashJoiner(new GetOrgNameFunction());
        
        // computer
        
        AffOrgMatchComputer mainSectionHashMatchComputer = new AffOrgMatchComputer();
        
        mainSectionHashMatchComputer.setAffOrgMatchVoters(createNameMainSectionHashBucketMatcherVoters());
        
        
        // matcher
        
        return new AffOrgMatcher(mainSectionHashBucketJoiner, mainSectionHashMatchComputer);
        
    }


    
    /**
     * Creates {@link AffOrgMatchVoter}s for {@link #createNameMainSectionHashBucketMatcher()} 
     */
    public static ImmutableList<AffOrgMatchVoter> createNameMainSectionHashBucketMatcherVoters() {
        
        return ImmutableList.of(
                createNameCountryStrictMatchVoter(0.981f, new GetOrgNameFunction()),
                createNameStrictCountryLooseMatchVoter(0.966f, new GetOrgNameFunction()),
                createSectionedNameStrictCountryLooseMatchVoter(0.978f, new GetOrgNameFunction()),
                createSectionedNameLevenshteinCountryLooseMatchVoter(0.964f, new GetOrgNameFunction()),
                createSectionedNameStrictCountryLooseMatchVoter(0.937f, new GetOrgShortNameFunction())
                );
    }
    
    
    
    /**
     * Returns {@link AffOrgMatcher} that uses hashing of affiliations and organizations to create buckets.<br/>
     * Hashes are computed based on the main section {@link AffMatchAffiliation#getAlternativeNames()}
     * and {@link AffMatchOrganization#getName()}.
     * 
     * @see MainSectionBucketHasher#hash(String)
     */
    public static AffOrgMatcher createAlternativeNameMainSectionHashBucketMatcher() {
        
        // joiner
        
        AffOrgHashBucketJoiner mainSectionHashBucketJoiner = createMainSectionAffOrgHashJoiner(new GetOrgAlternativeNamesFunction());
        
        // computer
        
        AffOrgMatchComputer mainSectionHashMatchComputer = new AffOrgMatchComputer();
        
        mainSectionHashMatchComputer.setAffOrgMatchVoters(createAlternativeNameMainSectionHashBucketMatcherVoters());
        
        // matcher
        
        return new AffOrgMatcher(mainSectionHashBucketJoiner, mainSectionHashMatchComputer);
    }
    
    /**
     * Creates {@link AffOrgMatchVoter}s for {@link #createAlternativeNameMainSectionHashBucketMatcher()} 
     */
    public static ImmutableList<AffOrgMatchVoter> createAlternativeNameMainSectionHashBucketMatcherVoters() {
        
        return ImmutableList.of(
                createNameCountryStrictMatchVoter(1f, new GetOrgAlternativeNamesFunction()),
                createNameStrictCountryLooseMatchVoter(1f, new GetOrgAlternativeNamesFunction()),
                createSectionedNameStrictCountryLooseMatchVoter(0.987f, new GetOrgAlternativeNamesFunction()),
                createSectionedNameLevenshteinCountryLooseMatchVoter(0.988f, new GetOrgAlternativeNamesFunction())
                );
    }
    
    
    /**
     * Returns {@link AffOrgMatcher} that uses hashing of affiliations and organizations to create buckets.<br/>
     * Hashes are computed based on main section of {@link AffMatchAffiliation#getOrganizationName()}
     * and {@link AffMatchOrganization#getShortName()}.
     * 
     * @see MainSectionBucketHasher#hash(String)
     */
    public static AffOrgMatcher createShortNameMainSectionHashBucketMatcher() {
        
        // joiner
        
        AffOrgHashBucketJoiner mainSectionHashBucketJoiner = createMainSectionAffOrgHashJoiner(new GetOrgShortNameFunction());
        
        // computer
        
        AffOrgMatchComputer mainSectionHashMatchComputer = new AffOrgMatchComputer();
        
        mainSectionHashMatchComputer.setAffOrgMatchVoters(createShortNameMainSectionHashBucketMatcherVoters());
        
        // matcher
        
        return new AffOrgMatcher(mainSectionHashBucketJoiner, mainSectionHashMatchComputer);
    }

    /**
     * Creates {@link AffOrgMatchVoter}s for {@link #createShortNameMainSectionHashBucketMatcher()} 
     */
    public static ImmutableList<AffOrgMatchVoter> createShortNameMainSectionHashBucketMatcherVoters() {
        
        return ImmutableList.of(
                createNameCountryStrictMatchVoter(1f, new GetOrgShortNameFunction()),
                createNameStrictCountryLooseMatchVoter(0.658f, new GetOrgShortNameFunction())
                );
    }
  
    
    //------------------------ PRIVATE --------------------------
    
    private static AffOrgHashBucketJoiner createMainSectionAffOrgHashJoiner(Function<AffMatchOrganization, List<String>> getOrgNamesFunction) {
        
        // affiliation hasher
        
        AffiliationOrgNameBucketHasher mainSectionAffBucketHasher = new AffiliationOrgNameBucketHasher();
        MainSectionBucketHasher mainSectionStringAffBucketHasher = new MainSectionBucketHasher();
        mainSectionStringAffBucketHasher.setFallbackSectionPickStrategy(FallbackSectionPickStrategy.LAST_SECTION);
        mainSectionAffBucketHasher.setStringHasher(mainSectionStringAffBucketHasher);
        
        // organization hasher
        
        OrganizationNameBucketHasher mainSectionOrgBucketHasher = new OrganizationNameBucketHasher();
        mainSectionOrgBucketHasher.setGetOrgNamesFunction(getOrgNamesFunction);
        MainSectionBucketHasher mainSectionStringOrgBucketHasher = new MainSectionBucketHasher();
        mainSectionStringOrgBucketHasher.setFallbackSectionPickStrategy(FallbackSectionPickStrategy.FIRST_SECTION);
        mainSectionOrgBucketHasher.setStringHasher(mainSectionStringOrgBucketHasher);
        
        // joiner
        
        AffOrgHashBucketJoiner mainSectionHashBucketJoiner = new AffOrgHashBucketJoiner();
        
        mainSectionHashBucketJoiner.setAffiliationBucketHasher(mainSectionAffBucketHasher);
        mainSectionHashBucketJoiner.setOrganizationBucketHasher(mainSectionOrgBucketHasher);
        return mainSectionHashBucketJoiner;
    }

    
}
