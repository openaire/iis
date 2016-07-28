package eu.dnetlib.iis.wf.affmatching.match;

import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createNameCountryStrictMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createNameStrictCountryLooseMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createSectionedNameLevenshteinCountryLooseMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createSectionedNameStrictCountryLooseMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.CommonWordsVoter.RatioRelation.WITH_REGARD_TO_AFF_WORDS;
import static eu.dnetlib.iis.wf.affmatching.match.voter.CommonWordsVoter.RatioRelation.WITH_REGARD_TO_ORG_WORDS;

import java.util.List;
import java.util.function.Function;

import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.ImmutableList;

import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgHashBucketJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.AffiliationOrgNameBucketHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.DocOrgRelationAffOrgJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.MainSectionBucketHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.MainSectionBucketHasher.FallbackSectionPickStrategy;
import eu.dnetlib.iis.wf.affmatching.bucket.OrganizationNameBucketHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.DocumentOrganizationCombiner;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.DocumentOrganizationFetcher;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.DocumentProjectFetcher;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.DocumentProjectMerger;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.IisDocumentProjectReader;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.IisInferredDocumentProjectReader;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.IisProjectOrganizationReader;
import eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CommonAffSectionWordsVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CommonSimilarWordCalculator;
import eu.dnetlib.iis.wf.affmatching.match.voter.CommonWordsVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CompositeMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CountryCodeLooseMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgAlternativeNamesFunction;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgNameFunction;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgShortNameFunction;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * Factory of {@link AffOrgMatcher} objects.
 * 
 * @author madryk
 */
public class AffOrgMatcherFactory {

    
    //------------------------ CONSTRUCTORS --------------------------
    
    private AffOrgMatcherFactory() { }
    
    
    //------------------------ LOGIC --------------------------
    
    
    //--------------------------------------------------------------------------------
    // DOC ORG REL MATCHER
    //--------------------------------------------------------------------------------

    /**
     * Returns {@link AffOrgMatcher} that uses document-organization relations to create buckets.<br/>
     * Document-organization relations are fetched from combined inputs of document-project and project-organization
     * relations.
     * 
     * @param inputAvroDocProjPath - source of document-project relations saved in avro files 
     *      (with {@link eu.dnetlib.iis.importer.schemas.DocumentToProject} schema)
     * @param inputAvroInferredDocProjPath - source of document-project relations inferred by iis
     *      saved in avro files (with {@link eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject} schema)
     * @param inputAvroProjOrgPath - source of project-document relations saved in avro files (with {@link ProjectToOrganization} schema)
     * @param inputDocProjConfidenceThreshold - minimal confidence level for {@link DocumentToProject} relations
     */
    public static AffOrgMatcher createDocOrgRelationMatcher(JavaSparkContext sparkContext, String inputAvroDocProjPath, String inputAvroInferredDocProjPath, 
            String inputAvroProjOrgPath, Float inputDocProjConfidenceThreshold) {
        
        // joiner
        
        DocumentOrganizationFetcher documentOrganizationFetcher = new DocumentOrganizationFetcher();
        
        DocumentProjectFetcher documentProjectFetcher = new DocumentProjectFetcher();
        documentProjectFetcher.setFirstDocumentProjectReader(new IisDocumentProjectReader());
        documentProjectFetcher.setSecondDocumentProjectReader(new IisInferredDocumentProjectReader());
        documentProjectFetcher.setDocumentProjectMerger(new DocumentProjectMerger());
        documentProjectFetcher.setSparkContext(sparkContext);
        documentProjectFetcher.setFirstDocProjPath(inputAvroDocProjPath);
        documentProjectFetcher.setSecondDocProjPath(inputAvroInferredDocProjPath);
        
        documentOrganizationFetcher.setDocumentProjectFetcher(documentProjectFetcher);
        
        documentOrganizationFetcher.setProjectOrganizationReader(new IisProjectOrganizationReader());
        documentOrganizationFetcher.setDocumentOrganizationCombiner(new DocumentOrganizationCombiner());
        documentOrganizationFetcher.setDocProjConfidenceLevelThreshold(inputDocProjConfidenceThreshold);
        documentOrganizationFetcher.setSparkContext(sparkContext);
        documentOrganizationFetcher.setProjOrgPath(inputAvroProjOrgPath);
        
        DocOrgRelationAffOrgJoiner docOrgRelationJoiner = new DocOrgRelationAffOrgJoiner();
        docOrgRelationJoiner.setDocumentOrganizationFetcher(documentOrganizationFetcher);
        
        // computer
        
        AffOrgMatchComputer docOrgRelationMatchComputer = new AffOrgMatchComputer();
        docOrgRelationMatchComputer.setAffOrgMatchVoters(createDocOrgRelationMatcherVoters());
        
        // matcher
        
        return createAffOrgMatcher(docOrgRelationJoiner, docOrgRelationMatchComputer);
    }

    /**
     * Creates {@link AffOrgMatchVoter}s for {@link #createDocOrgRelationMatcher(JavaSparkContext, String, String, String, Float)} 
     */
    public static ImmutableList<AffOrgMatchVoter> createDocOrgRelationMatcherVoters() {

        CommonWordsVoter commonOrgNameWordsVoter = new CommonWordsVoter(ImmutableList.of(',', ';'), 2, 0.7, WITH_REGARD_TO_ORG_WORDS);
        commonOrgNameWordsVoter.setCommonSimilarWordCalculator(new CommonSimilarWordCalculator(0.9));
        commonOrgNameWordsVoter.setMatchStrength(0.914f);
        
        CommonWordsVoter commonOrgAlternativeNameWordsVoter = new CommonWordsVoter(ImmutableList.of(',', ';'), 2, 0.7, WITH_REGARD_TO_ORG_WORDS);
        commonOrgAlternativeNameWordsVoter.setGetOrgNamesFunction(new GetOrgAlternativeNamesFunction());
        commonOrgAlternativeNameWordsVoter.setCommonSimilarWordCalculator(new CommonSimilarWordCalculator(0.9));
        commonOrgAlternativeNameWordsVoter.setMatchStrength(0.914f);
        
        
        CommonAffSectionWordsVoter commonAffNameSectionWordsVoter = new CommonAffSectionWordsVoter(ImmutableList.of(',', ';'), 1, 0.8);
        commonAffNameSectionWordsVoter.setCommonSimilarWordCalculator(new CommonSimilarWordCalculator(0.85));
        commonAffNameSectionWordsVoter.setMatchStrength(0.966f);
        
        CommonAffSectionWordsVoter commonAffAlternativeNameSectionWordsVoter = new CommonAffSectionWordsVoter(ImmutableList.of(',', ';'), 1, 0.8);
        commonAffAlternativeNameSectionWordsVoter.setGetOrgNamesFunction(new GetOrgAlternativeNamesFunction());
        commonAffAlternativeNameSectionWordsVoter.setCommonSimilarWordCalculator(new CommonSimilarWordCalculator(0.85));
        commonAffAlternativeNameSectionWordsVoter.setMatchStrength(1);
        
        
        return ImmutableList.of(
                createNameCountryStrictMatchVoter(1f, new GetOrgNameFunction()),
                createNameCountryStrictMatchVoter(1f, new GetOrgAlternativeNamesFunction()),
                createNameCountryStrictMatchVoter(1f, new GetOrgShortNameFunction()),
                
                createNameStrictCountryLooseMatchVoter(1f, new GetOrgNameFunction()),
                createNameStrictCountryLooseMatchVoter(1f, new GetOrgAlternativeNamesFunction()),
                createNameStrictCountryLooseMatchVoter(1f, new GetOrgShortNameFunction()),
                
                createSectionedNameStrictCountryLooseMatchVoter(1f, new GetOrgNameFunction()),
                createSectionedNameStrictCountryLooseMatchVoter(1f, new GetOrgAlternativeNamesFunction()),
                createSectionedNameStrictCountryLooseMatchVoter(1f, new GetOrgShortNameFunction()),
                
                createSectionedNameLevenshteinCountryLooseMatchVoter(1f, new GetOrgNameFunction()),
                createSectionedNameLevenshteinCountryLooseMatchVoter(1f, new GetOrgAlternativeNamesFunction()),
                
                commonOrgNameWordsVoter,
                commonOrgAlternativeNameWordsVoter,
                
                commonAffNameSectionWordsVoter,
                commonAffAlternativeNameSectionWordsVoter
                );
    }
    
    
    //--------------------------------------------------------------------------------
    // MAIN SECTION HASH BUCKET MATCHER
    //--------------------------------------------------------------------------------

    
  
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
        
        return createAffOrgMatcher(mainSectionHashBucketJoiner, mainSectionHashMatchComputer);
    }


    
    /**
     * Creates {@link AffOrgMatchVoter}s for {@link #createNameMainSectionHashBucketMatcher()} 
     */
    public static ImmutableList<AffOrgMatchVoter> createNameMainSectionHashBucketMatcherVoters() {
        
        return ImmutableList.of(
                createNameCountryStrictMatchVoter(0.981f, new GetOrgNameFunction()),
                createNameStrictCountryLooseMatchVoter(0.966f, new GetOrgNameFunction()),
                createSectionedNameStrictCountryLooseMatchVoter(0.988f, new GetOrgNameFunction()),
                createSectionedNameLevenshteinCountryLooseMatchVoter(0.983f, new GetOrgNameFunction()),
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
        
        return createAffOrgMatcher(mainSectionHashBucketJoiner, mainSectionHashMatchComputer);
    }
    
    /**
     * Creates {@link AffOrgMatchVoter}s for {@link #createAlternativeNameMainSectionHashBucketMatcher()} 
     */
    public static ImmutableList<AffOrgMatchVoter> createAlternativeNameMainSectionHashBucketMatcherVoters() {
        
        return ImmutableList.of(
                createNameCountryStrictMatchVoter(1f, new GetOrgAlternativeNamesFunction()),
                createNameStrictCountryLooseMatchVoter(1f, new GetOrgAlternativeNamesFunction()),
                createSectionedNameStrictCountryLooseMatchVoter(1f, new GetOrgAlternativeNamesFunction()),
                createSectionedNameLevenshteinCountryLooseMatchVoter(1f, new GetOrgAlternativeNamesFunction())
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
        
        return createAffOrgMatcher(mainSectionHashBucketJoiner, mainSectionHashMatchComputer);
    }

    /**
     * Creates {@link AffOrgMatchVoter}s for {@link #createShortNameMainSectionHashBucketMatcher()} 
     */
    public static ImmutableList<AffOrgMatchVoter> createShortNameMainSectionHashBucketMatcherVoters() {
        
        return ImmutableList.of(
                createNameCountryStrictMatchVoter(1f, new GetOrgShortNameFunction()),
                createNameStrictCountryLooseMatchVoter(0.962f, new GetOrgShortNameFunction())
                );
    }
  
    
    //--------------------------------------------------------------------------------
    // FIRST WORDS HASH BUCKET MATCHER
    //--------------------------------------------------------------------------------
    
    
    /**
     * Returns {@link AffOrgMatcher} that uses hashing of affiliations and organizations to create buckets.<br/>
     * Hashes are computed based on first letters of first words of {@link AffMatchAffiliation#getOrganizationName()}
     * and {@link AffMatchOrganization#getName()}.
     */
    public static AffOrgMatcher createNameFirstWordsHashBucketMatcher() {
        
        // joiner
        
        AffOrgHashBucketJoiner firstWordsHashBucketJoiner = createFirstWordsAffOrgHashBucketJoiner(new GetOrgNameFunction());
        
        // computer
        
        AffOrgMatchComputer firstWordsHashMatchComputer = new AffOrgMatchComputer();
        
        firstWordsHashMatchComputer.setAffOrgMatchVoters(createNameFirstWordsHashBucketMatcherVoters());
        
        // matcher
        
        return createAffOrgMatcher(firstWordsHashBucketJoiner, firstWordsHashMatchComputer);
    }
    
    
    /**
     * Creates {@link AffOrgMatchVoter}s for {@link #createNameFirstWordsHashBucketMatcher()} 
     */
    public static ImmutableList<AffOrgMatchVoter> createNameFirstWordsHashBucketMatcherVoters() {
        
        CommonWordsVoter commonOrgNameWordsVoter = new CommonWordsVoter(ImmutableList.of(',', ';'), 2, 0.7, WITH_REGARD_TO_ORG_WORDS);
        commonOrgNameWordsVoter.setCommonSimilarWordCalculator(new CommonSimilarWordCalculator(0.9));
        
        CommonWordsVoter commonAffNameWordsVoter = new CommonWordsVoter(ImmutableList.of(',', ';'), 2, 0.8, WITH_REGARD_TO_AFF_WORDS);
        commonAffNameWordsVoter.setCommonSimilarWordCalculator(new CommonSimilarWordCalculator(0.9));
        
        
        CompositeMatchVoter commonAffOrgNameWordsVoter = new CompositeMatchVoter(ImmutableList.of(new CountryCodeLooseMatchVoter(), commonOrgNameWordsVoter, commonAffNameWordsVoter));
        commonAffOrgNameWordsVoter.setMatchStrength(0.878f);
        
        return ImmutableList.of(
                createNameCountryStrictMatchVoter(0.981f, new GetOrgNameFunction()),
                createNameStrictCountryLooseMatchVoter(0.966f, new GetOrgNameFunction()),
                createSectionedNameStrictCountryLooseMatchVoter(0.943f, new GetOrgNameFunction()),
                createSectionedNameLevenshteinCountryLooseMatchVoter(0.934f, new GetOrgNameFunction()),
                createSectionedNameStrictCountryLooseMatchVoter(0.882f, new GetOrgShortNameFunction()),
                commonAffOrgNameWordsVoter);
    }
    
    
    
    
    //------------------------ PRIVATE --------------------------
    
    private static AffOrgMatcher createAffOrgMatcher(AffOrgJoiner joiner, AffOrgMatchComputer computer) {
        
        AffOrgMatcher affOrgMatcher = new AffOrgMatcher();
        affOrgMatcher.setAffOrgJoiner(joiner);
        affOrgMatcher.setAffOrgMatchComputer(computer);
        
        return affOrgMatcher;
        
        
        
    }
    
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

    
    private static AffOrgHashBucketJoiner createFirstWordsAffOrgHashBucketJoiner(Function<AffMatchOrganization, List<String>> getOrgNamesFunction) {
     
        AffOrgHashBucketJoiner firstWordsHashBucketJoiner = new AffOrgHashBucketJoiner();
    
        OrganizationNameBucketHasher orgNameBucketHasher = new OrganizationNameBucketHasher();
        orgNameBucketHasher.setGetOrgNamesFunction(getOrgNamesFunction);
        
        firstWordsHashBucketJoiner.setOrganizationBucketHasher(orgNameBucketHasher);
        
        return firstWordsHashBucketJoiner;
    
    }
    
}
