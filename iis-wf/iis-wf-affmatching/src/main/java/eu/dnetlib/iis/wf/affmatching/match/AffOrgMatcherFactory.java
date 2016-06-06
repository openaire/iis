package eu.dnetlib.iis.wf.affmatching.match;

import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createNameCountryStrictMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createNameStrictCountryLooseMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createSectionedNameLevenshteinCountryLooseMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createSectionedNameStrictCountryLooseMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createSectionedShortNameStrictCountryLooseMatchVoter;

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
        return ImmutableList.of(
                createNameCountryStrictMatchVoter(),
                createNameStrictCountryLooseMatchVoter(),
                createSectionedNameStrictCountryLooseMatchVoter(),
                createSectionedNameLevenshteinCountryLooseMatchVoter(),
                createSectionedShortNameStrictCountryLooseMatchVoter());
    }
    
    
    
    /**
     * Returns {@link AffOrgMatcher} that uses hashing of affiliations and organizations to create buckets.<br/>
     * Hashes are computed based on main section of {@link AffMatchAffiliation#getOrganizationName()}
     * and {@link AffMatchOrganization#getName()}.
     * 
     * @see MainSectionBucketHasher#hash(String)
     */
    public static AffOrgMatcher createMainSectionHashBucketMatcher() {
        
        // affiliation hasher
        
        AffiliationOrgNameBucketHasher mainSectionAffBucketHasher = new AffiliationOrgNameBucketHasher();
        MainSectionBucketHasher mainSectionStringAffBucketHasher = new MainSectionBucketHasher();
        mainSectionStringAffBucketHasher.setFallbackSectionPickStrategy(FallbackSectionPickStrategy.LAST_SECTION);
        mainSectionAffBucketHasher.setStringHasher(mainSectionStringAffBucketHasher);
        
        // organization hasher
        
        OrganizationNameBucketHasher mainSectionOrgBucketHasher = new OrganizationNameBucketHasher();
        MainSectionBucketHasher mainSectionStringOrgBucketHasher = new MainSectionBucketHasher();
        mainSectionStringOrgBucketHasher.setFallbackSectionPickStrategy(FallbackSectionPickStrategy.FIRST_SECTION);
        mainSectionOrgBucketHasher.setStringHasher(mainSectionStringOrgBucketHasher);
        
        // joiner
        
        AffOrgHashBucketJoiner mainSectionHashBucketJoiner = new AffOrgHashBucketJoiner();
        
        mainSectionHashBucketJoiner.setAffiliationBucketHasher(mainSectionAffBucketHasher);
        mainSectionHashBucketJoiner.setOrganizationBucketHasher(mainSectionOrgBucketHasher);
        
        // computer
        
        AffOrgMatchComputer mainSectionHashMatchComputer = new AffOrgMatchComputer();
        
        mainSectionHashMatchComputer.setAffOrgMatchVoters(createMainSectionHashBucketMatcherVoters());
        
        // matcher
        
        return createAffOrgMatcher(mainSectionHashBucketJoiner, mainSectionHashMatchComputer);
    }
    
    /**
     * Creates {@link AffOrgMatchVoter}s for {@link #createMainSectionHashBucketMatcher()} 
     */
    public static ImmutableList<AffOrgMatchVoter> createMainSectionHashBucketMatcherVoters() {
        return ImmutableList.of(
                createNameCountryStrictMatchVoter(),
                createNameStrictCountryLooseMatchVoter(),
                createSectionedNameStrictCountryLooseMatchVoter(),
                createSectionedNameLevenshteinCountryLooseMatchVoter(),
                createSectionedShortNameStrictCountryLooseMatchVoter());
    }
    
    
    /**
     * Returns {@link AffOrgMatcher} that uses hashing of affiliations and organizations to create buckets.<br/>
     * Hashes are computed based on first letters of first words of {@link AffMatchAffiliation#getOrganizationName()}
     * and {@link AffMatchOrganization#getName()}.
     */
    public static AffOrgMatcher createFirstWordsHashBucketMatcher() {
        
        // joiner
        
        AffOrgJoiner firstWordsHashBucketJoiner = new AffOrgHashBucketJoiner();
        
        // computer
        
        AffOrgMatchComputer firstWordsHashMatchComputer = new AffOrgMatchComputer();
        
        firstWordsHashMatchComputer.setAffOrgMatchVoters(createFirstWordsHashBucketMatcherVoters());
        
        // matcher
        
        return createAffOrgMatcher(firstWordsHashBucketJoiner, firstWordsHashMatchComputer);
    }
    
    
    /**
     * Creates {@link AffOrgMatchVoter}s for {@link #createFirstWordsHashBucketMatcher()} 
     */
    public static ImmutableList<AffOrgMatchVoter> createFirstWordsHashBucketMatcherVoters() {
        return ImmutableList.of(
                createNameCountryStrictMatchVoter(),
                createNameStrictCountryLooseMatchVoter(),
                createSectionedNameStrictCountryLooseMatchVoter(),
                createSectionedNameLevenshteinCountryLooseMatchVoter(),
                createSectionedShortNameStrictCountryLooseMatchVoter());
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private static AffOrgMatcher createAffOrgMatcher(AffOrgJoiner joiner, AffOrgMatchComputer computer) {
        
        AffOrgMatcher affOrgMatcher = new AffOrgMatcher();
        affOrgMatcher.setAffOrgJoiner(joiner);
        affOrgMatcher.setAffOrgMatchComputer(computer);
        
        return affOrgMatcher;
    }
    
}
