package eu.dnetlib.iis.wf.affmatching.match;

import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.assertCommonAffOrgSectionWordsVoter;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.assertCommonWordsVoter;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.assertCompositeVoter;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.assertNameStrictWithCharFilteringMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.assertSectionedNameLevenshteinMatchVoter;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.assertVoterGetOrgNamesFunction;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchVoterAssertUtils.getInternalVoter;
import static eu.dnetlib.iis.wf.affmatching.match.voter.CommonWordsVoter.RatioRelation.WITH_REGARD_TO_ORG_WORDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.powermock.reflect.Whitebox.getInternalState;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.DocOrgRelationAffOrgJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.DocumentOrganizationCombiner;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.DocumentOrganizationFetcher;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.DocumentProjectFetcher;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.DocumentProjectMerger;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.IisDocumentProjectReader;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.IisInferredDocumentProjectReader;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.IisProjectOrganizationReader;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.ProjectOrganizationReader;
import eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CountryCodeLooseMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CountryCodeStrictMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgAlternativeNamesFunction;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgNameFunction;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgShortNameFunction;
import eu.dnetlib.iis.wf.affmatching.match.voter.NameStrictWithCharFilteringMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.SectionedNameLevenshteinMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.SectionedNameStrictMatchVoter;

/**
* @author Łukasz Dumiszewski
*/

public class DocOrgRelationMatcherFactoryTest {

    
    private static final double PRECISION = 1e10-6;
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void createDocOrgRelationMatcher() {
        
        // given
        
        JavaSparkContext sparkContext = Mockito.mock(JavaSparkContext.class);
        String inputAvroDocProjPath = "/path/docproj/";
        String inputAvroInferredDocProjPath = "/path/docproj/inferred/";
        String inputAvroProjOrgPath = "/path/projorg/";
        Float inputDocProjConfidenceThreshold = 0.4f;
        
        
        // execute
        
        AffOrgMatcher matcher = DocOrgRelationMatcherFactory.createDocOrgRelationMatcher(sparkContext, inputAvroDocProjPath, inputAvroInferredDocProjPath, inputAvroProjOrgPath, inputDocProjConfidenceThreshold);
        
        
        // assert
        
        AffOrgJoiner joiner = getInternalState(matcher, AffOrgJoiner.class);
        assertTrue(joiner instanceof DocOrgRelationAffOrgJoiner);
        
        DocumentOrganizationFetcher docOrgFetcher = getInternalState(joiner, DocumentOrganizationFetcher.class);
        
        assertTrue(getInternalState(docOrgFetcher, ProjectOrganizationReader.class) instanceof IisProjectOrganizationReader);
        assertNotNull(getInternalState(docOrgFetcher, DocumentOrganizationCombiner.class));
        
        float actualDocProjConfidenceLevelThreshold = getInternalState(docOrgFetcher, "docProjConfidenceLevelThreshold");
        assertEquals(inputDocProjConfidenceThreshold, actualDocProjConfidenceLevelThreshold, PRECISION);
        assertEquals(sparkContext, getInternalState(docOrgFetcher, JavaSparkContext.class));
        assertEquals(inputAvroProjOrgPath, getInternalState(docOrgFetcher, "projOrgPath"));
        
        
        DocumentProjectFetcher docProjFetcher = getInternalState(docOrgFetcher, DocumentProjectFetcher.class);
        
        assertTrue(getInternalState(docProjFetcher, "firstDocumentProjectReader") instanceof IisDocumentProjectReader);
        assertTrue(getInternalState(docProjFetcher, "secondDocumentProjectReader") instanceof IisInferredDocumentProjectReader);
        
        assertEquals(inputAvroDocProjPath, getInternalState(docProjFetcher, "firstDocProjPath"));
        assertEquals(inputAvroInferredDocProjPath, getInternalState(docProjFetcher, "secondDocProjPath"));
        
        assertNotNull(getInternalState(docProjFetcher, DocumentProjectMerger.class));
        
        
        AffOrgMatchComputer computer = getInternalState(matcher, AffOrgMatchComputer.class);
        
        List<AffOrgMatchVoter> voters = getInternalState(computer, "affOrgMatchVoters");
        
        assertDocOrgRelationMatcherVoters(voters);
    }
    
    
    @Test
    public void createDocOrgRelationMatcherVoters() {
        
        // execute
        
        List<AffOrgMatchVoter> voters = DocOrgRelationMatcherFactory.createDocOrgRelationMatcherVoters();
        
        // assert
        
        assertDocOrgRelationMatcherVoters(voters);
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void assertDocOrgRelationMatcherVoters(List<AffOrgMatchVoter> voters) {
        
        assertEquals(15, voters.size());
        
        assertCompositeVoter(voters.get(0), CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(0), 1), ImmutableList.of(',', ';'), GetOrgNameFunction.class);
        
        assertCompositeVoter(voters.get(1), CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(1), 1), ImmutableList.of(',', ';'), GetOrgAlternativeNamesFunction.class);
        
        assertCompositeVoter(voters.get(2), CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(2), 1), ImmutableList.of(',', ';'), GetOrgShortNameFunction.class);
        
        
        assertCompositeVoter(voters.get(3), CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(3), 1), ImmutableList.of(',', ';'), GetOrgNameFunction.class);
        
        assertCompositeVoter(voters.get(4), CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(4), 1), ImmutableList.of(',', ';'), GetOrgAlternativeNamesFunction.class);
        
        assertCompositeVoter(voters.get(5), CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(5), 1), ImmutableList.of(',', ';'), GetOrgShortNameFunction.class);
        
        
        assertCompositeVoter(voters.get(6), CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
        assertVoterGetOrgNamesFunction(getInternalVoter(voters.get(6), 1), GetOrgNameFunction.class);
        
        assertCompositeVoter(voters.get(7), CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
        assertVoterGetOrgNamesFunction(getInternalVoter(voters.get(7), 1), GetOrgAlternativeNamesFunction.class);
        
        assertCompositeVoter(voters.get(8), CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
        assertVoterGetOrgNamesFunction(getInternalVoter(voters.get(8), 1), GetOrgShortNameFunction.class);
        
        
        assertCompositeVoter(voters.get(9), CountryCodeLooseMatchVoter.class, SectionedNameLevenshteinMatchVoter.class);
        assertSectionedNameLevenshteinMatchVoter(getInternalVoter(voters.get(9), 1), 0.9f, GetOrgNameFunction.class);
        
        assertCompositeVoter(voters.get(10), CountryCodeLooseMatchVoter.class, SectionedNameLevenshteinMatchVoter.class);
        assertSectionedNameLevenshteinMatchVoter(getInternalVoter(voters.get(10) ,1), 0.9f, GetOrgAlternativeNamesFunction.class);
        
        
        assertCommonWordsVoter(voters.get(11), ImmutableList.of(',', ';'), 0.7f, WITH_REGARD_TO_ORG_WORDS, 0.9f, 2, GetOrgNameFunction.class);
        
        assertCommonWordsVoter(voters.get(12), ImmutableList.of(',', ';'), 0.7f, WITH_REGARD_TO_ORG_WORDS, 0.9f, 2, GetOrgAlternativeNamesFunction.class);
        
        
        assertCommonAffOrgSectionWordsVoter(voters.get(13), ImmutableList.of(',', ';'), 0.8f, 0.85f, 1, GetOrgNameFunction.class);
        
        assertCommonAffOrgSectionWordsVoter(voters.get(14), ImmutableList.of(',', ';'), 0.8f, 0.85f, 1, GetOrgAlternativeNamesFunction.class);
    }
   

}
