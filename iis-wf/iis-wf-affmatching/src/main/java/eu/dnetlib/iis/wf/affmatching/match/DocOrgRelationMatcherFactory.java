package eu.dnetlib.iis.wf.affmatching.match;

import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createNameCountryStrictMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createNameStrictCountryLooseMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createSectionedNameLevenshteinCountryLooseMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVotersFactory.createSectionedNameStrictCountryLooseMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.CommonWordsVoter.RatioRelation.WITH_REGARD_TO_ORG_WORDS;

import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.ImmutableList;

import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.bucket.DocOrgRelationAffOrgJoiner;
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
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgAlternativeNamesFunction;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgNameFunction;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgShortNameFunction;

/**
* @author Łukasz Dumiszewski
*/

public class DocOrgRelationMatcherFactory {
    
    
    
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

        return new AffOrgMatcher(docOrgRelationJoiner, docOrgRelationMatchComputer);
        
    }

    /**
     * Creates {@link AffOrgMatchVoter}s for {@link #createDocOrgRelationMatcher(JavaSparkContext, String, String, String, Float)} 
     */
    public static ImmutableList<AffOrgMatchVoter> createDocOrgRelationMatcherVoters() {

        CommonWordsVoter commonOrgNameWordsVoter = new CommonWordsVoter(ImmutableList.of(',', ';'), 2, 0.71, WITH_REGARD_TO_ORG_WORDS);
        commonOrgNameWordsVoter.setCommonSimilarWordCalculator(new CommonSimilarWordCalculator(0.9));
        commonOrgNameWordsVoter.setMatchStrength(0.916f);
        
        CommonWordsVoter commonOrgAlternativeNameWordsVoter = new CommonWordsVoter(ImmutableList.of(',', ';'), 2, 0.71, WITH_REGARD_TO_ORG_WORDS);
        commonOrgAlternativeNameWordsVoter.setGetOrgNamesFunction(new GetOrgAlternativeNamesFunction());
        commonOrgAlternativeNameWordsVoter.setCommonSimilarWordCalculator(new CommonSimilarWordCalculator(0.9));
        commonOrgAlternativeNameWordsVoter.setMatchStrength(0.976f);
        
        
        CommonAffSectionWordsVoter commonAffNameSectionWordsVoter = new CommonAffSectionWordsVoter(ImmutableList.of(',', ';'), 1, 0.81);
        commonAffNameSectionWordsVoter.setCommonSimilarWordCalculator(new CommonSimilarWordCalculator(0.85));
        commonAffNameSectionWordsVoter.setMatchStrength(0.966f);
        
        CommonAffSectionWordsVoter commonAffAlternativeNameSectionWordsVoter = new CommonAffSectionWordsVoter(ImmutableList.of(',', ';'), 1, 0.81);
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
   
}
