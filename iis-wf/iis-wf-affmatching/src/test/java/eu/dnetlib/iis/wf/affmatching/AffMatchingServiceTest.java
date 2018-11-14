package eu.dnetlib.iis.wf.affmatching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.wf.affmatching.match.AffMatchResultChooser;
import eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcher;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import eu.dnetlib.iis.wf.affmatching.normalize.AffMatchAffiliationNormalizer;
import eu.dnetlib.iis.wf.affmatching.normalize.AffMatchOrganizationNormalizer;
import eu.dnetlib.iis.wf.affmatching.orgalternativenames.AffMatchOrganizationAltNameFiller;
import eu.dnetlib.iis.wf.affmatching.read.AffiliationReader;
import eu.dnetlib.iis.wf.affmatching.read.OrganizationReader;
import eu.dnetlib.iis.wf.affmatching.write.AffMatchResultWriter;
import scala.Tuple2;

/**
* @author Łukasz Dumiszewski
*/
@RunWith(MockitoJUnitRunner.class)
public class AffMatchingServiceTest {

    
    //----------------------------------- SERVICES -------------------------------------------
    
    @InjectMocks
    private AffMatchingService affMatchingService = new AffMatchingService();
    
    
    @Mock
    private OrganizationReader organizationReader;
    
    @Mock
    private AffiliationReader affiliationReader;
    
    @Mock
    private AffMatchAffiliationNormalizer affMatchAffiliationNormalizer;
    
    @Mock
    private AffMatchOrganizationNormalizer affMatchOrganizationNormalizer;
    
    @Mock
    private AffMatchOrganizationAltNameFiller affMatchOrganizationAltNameFiller;
    
    @Mock
    private AffOrgMatcher affOrgMatcher1;
    
    @Mock
    private AffOrgMatcher affOrgMatcher2;
    
    @Mock
    private AffMatchResultChooser affMatchResultChooser;
    
    @Mock
    private AffMatchResultWriter affMatchResultWriter;

    
    
    //----------------------------------- DATA -------------------------------------------

    @Mock
    private JavaSparkContext sc;
    
    private String inputAffPath = "/input/affs";
    
    private String inputOrgPath = "/input/orgs";
    
    private String outputPath = "/output";
    
    private String outputReportPath = "/outputReport";
    
    
    @Mock
    private JavaRDD<AffMatchAffiliation> affiliations;
    
    @Mock
    private JavaRDD<AffMatchOrganization> organizations;


    @Mock
    private JavaRDD<AffMatchAffiliation> normalizedAffiliations;
    
    @Mock
    private JavaRDD<AffMatchOrganization> normalizedOrganizations;


    @Mock
    private JavaRDD<AffMatchAffiliation> normalizedAndFilteredAffiliations;
    
    @Mock
    private JavaRDD<AffMatchOrganization> normalizedAndFilteredOrganizations;

    @Mock
    private JavaRDD<AffMatchOrganization> altNamesFilledOrganizations;
    
    
    @Mock
    private JavaPairRDD<Tuple2<String, String>, AffMatchResult> parallelizedAffMatchResults;
    
    @Mock
    private JavaRDD<AffMatchResult> matchedAffOrgs1;

    @Mock
    private JavaPairRDD<Tuple2<String, String>, AffMatchResult> matchedAffOrgsWithKey1;
    
    @Mock
    private JavaRDD<AffMatchResult> matchedAffOrgs2;
    
    @Mock
    private JavaPairRDD<Tuple2<String, String>, AffMatchResult> matchedAffOrgsWithKey2;

    
    @Mock
    private JavaPairRDD<Tuple2<String, String>, AffMatchResult> allAffMatchResults1;
    
    @Mock
    private JavaPairRDD<Tuple2<String, String>, AffMatchResult> allAffMatchResults2;
    
    
    @Mock
    private JavaPairRDD<Tuple2<String, String>, AffMatchResult> allUniqueAffMatchResults;
    
    @Mock
    private JavaRDD<AffMatchResult> allUniqueAffMatchResultsValues;
    
    
    
    //----------------------------------- FUNCTIONS -------------------------------------------
    
    @Captor
    private ArgumentCaptor<Function<AffMatchAffiliation, AffMatchAffiliation>> affNormalizingFunction;
    
    @Captor
    private ArgumentCaptor<Function<AffMatchOrganization, AffMatchOrganization>> orgNormalizingFunction;
    
    @Captor
    private ArgumentCaptor<Function<AffMatchAffiliation, Boolean>> affFilterFunction;
    
    @Captor
    private ArgumentCaptor<Function<AffMatchOrganization, Boolean>> orgFilterFunction;
    
    @Captor
    private ArgumentCaptor<Function<AffMatchOrganization, AffMatchOrganization>> orgFillAltNamesFunction;
    
    @Captor
    private ArgumentCaptor<Function<AffMatchResult, Tuple2<String, String>>> matchedAffOrgs1KeyByFunction;
    
    @Captor
    private ArgumentCaptor<Function<AffMatchResult, Tuple2<String, String>>> matchedAffOrgs2KeyByFunction;
    
    @Captor
    private ArgumentCaptor<Function2<AffMatchResult, AffMatchResult, AffMatchResult>> allAffMatchResultsReduceFunction;
    
    
    
    @Before
    public void before() {
        
        affMatchingService.setAffOrgMatchers(Lists.newArrayList(affOrgMatcher1, affOrgMatcher2));
        affMatchingService.setNumberOfEmittedFiles(1);
        
    }
    
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test(expected = NullPointerException.class)
    public void matchAffiliations_sparkContext_null() {
        
        // execute
        
        affMatchingService.matchAffiliations(null, inputAffPath, inputOrgPath, outputPath, outputReportPath);
        
    }
    

    @Test(expected = IllegalArgumentException.class)
    public void matchAffiliations_inputAffPath_blank() {
        
        // execute
        
        affMatchingService.matchAffiliations(sc, "  ", inputOrgPath, outputPath, outputReportPath);
        
    }


    @Test(expected = IllegalArgumentException.class)
    public void matchAffiliations_inputOrgPath_blank() {
        
        // execute
        
        affMatchingService.matchAffiliations(sc, inputAffPath, " ", outputPath, outputReportPath);
        
    }


    @Test(expected = IllegalArgumentException.class)
    public void matchAffiliations_outputPath_blank() {
        
        // execute
        
        affMatchingService.matchAffiliations(sc, inputAffPath, inputOrgPath, "   ", outputReportPath);
        
    }


    @Test(expected = NullPointerException.class)
    public void matchAffiliations_affiliationReader_null() {
        
        // given
        
        affMatchingService.setAffiliationReader(null);
        
        
        // execute
        
        affMatchingService.matchAffiliations(sc, inputAffPath, inputOrgPath, outputPath, outputReportPath);
        
    }

    
    @Test(expected = NullPointerException.class)
    public void matchAffiliations_organizationReader_null() {
        
        // given
        
        affMatchingService.setOrganizationReader(null);
        
        
        // execute
        
        affMatchingService.matchAffiliations(sc, inputAffPath, inputOrgPath, outputPath, outputReportPath);
        
    }
    

    @Test(expected = NullPointerException.class)
    public void matchAffiliations_affMatchResultWriter_null() {
        
        // given
        
        affMatchingService.setAffMatchResultWriter(null);
        
        
        // execute
        
        affMatchingService.matchAffiliations(sc, inputAffPath, inputOrgPath, outputPath, outputReportPath);
        
    }

    
    @Test(expected = IllegalStateException.class)
    public void matchAffiliations_affOrgMatchers_empty() {
        
        // given
        
        affMatchingService.setAffOrgMatchers(Lists.newArrayList());
        
        
        // execute
        
        affMatchingService.matchAffiliations(sc, inputAffPath, inputOrgPath, outputPath, outputReportPath);
        
    }

    
    @Test
    public void matchAffiliations() throws Exception {
        
        // given
        
        when(affiliationReader.readAffiliations(sc, inputAffPath)).thenReturn(affiliations);
        when(organizationReader.readOrganizations(sc, inputOrgPath)).thenReturn(organizations);
        
        doReturn(normalizedAffiliations).when(affiliations).map(Mockito.any());
        doReturn(normalizedOrganizations).when(organizations).map(Mockito.any());
        
        when(normalizedAffiliations.filter(Mockito.any())).thenReturn(normalizedAndFilteredAffiliations);
        when(normalizedOrganizations.filter(Mockito.any())).thenReturn(normalizedAndFilteredOrganizations);
    
        doReturn(altNamesFilledOrganizations).when(normalizedAndFilteredOrganizations).map(Mockito.any());
        
        
        //--- matching
        
        doReturn(parallelizedAffMatchResults).when(sc).parallelizePairs(new ArrayList<>());
        
        //- first matcher
        when(affOrgMatcher1.match(normalizedAndFilteredAffiliations, altNamesFilledOrganizations)).thenReturn(matchedAffOrgs1);
        doReturn(matchedAffOrgsWithKey1).when(matchedAffOrgs1).keyBy(Mockito.any());
        when(parallelizedAffMatchResults.union(matchedAffOrgsWithKey1)).thenReturn(allAffMatchResults1);
        
        //- second matcher
        when(affOrgMatcher2.match(normalizedAndFilteredAffiliations, altNamesFilledOrganizations)).thenReturn(matchedAffOrgs2);
        doReturn(matchedAffOrgsWithKey2).when(matchedAffOrgs2).keyBy(Mockito.any());
        when(allAffMatchResults1.union(matchedAffOrgsWithKey2)).thenReturn(allAffMatchResults2);
        
        //- pick unique matches
        doReturn(allUniqueAffMatchResults).when(allAffMatchResults2).reduceByKey(Mockito.any());
        when(allUniqueAffMatchResults.values()).thenReturn(allUniqueAffMatchResultsValues);
        
        // execute
        
        affMatchingService.matchAffiliations(sc, inputAffPath, inputOrgPath, outputPath, outputReportPath);
        
        
        // assert
        
        verify(affMatchResultWriter).write(sc, allUniqueAffMatchResultsValues, outputPath, outputReportPath, 1);
        
        verify(affiliationReader).readAffiliations(sc, inputAffPath);
        verify(organizationReader).readOrganizations(sc, inputOrgPath);
        
        verify(affiliations).map(affNormalizingFunction.capture());
        assertAffNormalizingFunction(affNormalizingFunction.getValue());

        verify(organizations).map(orgNormalizingFunction.capture());
        assertOrgNormalizingFunction(orgNormalizingFunction.getValue());

        verify(normalizedAffiliations).filter(affFilterFunction.capture());
        assertAffFilterFunction(affFilterFunction.getValue());

        verify(normalizedOrganizations).filter(orgFilterFunction.capture());
        assertOrgFilterFunction(orgFilterFunction.getValue());

        verify(normalizedAndFilteredOrganizations).map(orgFillAltNamesFunction.capture());
        assertOrgFillAltNamesFunctionFunction(orgFillAltNamesFunction.getValue());

        verify(matchedAffOrgs1).keyBy(matchedAffOrgs1KeyByFunction.capture());
        assertMatchedAffOrgsKeyByFunction(matchedAffOrgs1KeyByFunction.getValue());

        verify(matchedAffOrgs2).keyBy(matchedAffOrgs2KeyByFunction.capture());
        assertMatchedAffOrgsKeyByFunction(matchedAffOrgs2KeyByFunction.getValue());

        verify(allAffMatchResults2).reduceByKey(allAffMatchResultsReduceFunction.capture());
        assertAllAffMatchResultsReduceFunction(allAffMatchResultsReduceFunction.getValue());

    }
    
    
    
    //------------------------ PRIVATE --------------------------
    
    private void assertAffFilterFunction(Function<AffMatchAffiliation, Boolean> function) throws Exception {
        
        // given
        
        AffMatchAffiliation affMatchAff1 = mock(AffMatchAffiliation.class);
        when(affMatchAff1.getOrganizationName()).thenReturn("ICM");
        
        AffMatchAffiliation affMatchAff2 = mock(AffMatchAffiliation.class);
        when(affMatchAff2.getOrganizationName()).thenReturn(" ");
        
        
        // execute & assert
        
        assertTrue(function.call(affMatchAff1));
        assertFalse(function.call(affMatchAff2));
        
    }

    
    private void assertOrgFilterFunction(Function<AffMatchOrganization, Boolean> function) throws Exception {
        
        // given
        
        AffMatchOrganization affMatchOrg1 = mock(AffMatchOrganization.class);
        when(affMatchOrg1.getName()).thenReturn("ICM");
        
        AffMatchOrganization affMatchOrg2 = mock(AffMatchOrganization.class);
        when(affMatchOrg2.getName()).thenReturn(" ");
        
        
        // execute & assert
        
        assertTrue(function.call(affMatchOrg1));
        assertFalse(function.call(affMatchOrg2));
        
    }

    
    private void assertAffNormalizingFunction(Function<AffMatchAffiliation, AffMatchAffiliation> function) throws Exception {
        
        // given
        
        AffMatchAffiliation aff1 = mock(AffMatchAffiliation.class);
        AffMatchAffiliation aff2 = mock(AffMatchAffiliation.class);
        when(affMatchAffiliationNormalizer.normalize(aff1)).thenReturn(aff2);
        
        // execute & assert
        
        assertTrue(aff2 == function.call(aff1));
        
    }

    
    private void assertOrgNormalizingFunction(Function<AffMatchOrganization, AffMatchOrganization> function) throws Exception {
        
        // given
        
        AffMatchOrganization org1 = mock(AffMatchOrganization.class);
        AffMatchOrganization org2 = mock(AffMatchOrganization.class);
        when(affMatchOrganizationNormalizer.normalize(org1)).thenReturn(org2);
        
        // execute & assert
        
        assertTrue(org2 == function.call(org1));
        
    }

    private void assertOrgFillAltNamesFunctionFunction(Function<AffMatchOrganization, AffMatchOrganization> function) throws Exception {
        
        // given
        
        AffMatchOrganization org1 = mock(AffMatchOrganization.class);
        AffMatchOrganization org2 = mock(AffMatchOrganization.class);
        when(affMatchOrganizationAltNameFiller.fillAlternativeNames(org1)).thenReturn(org2);
        
        // execute & assert
        
        assertTrue(org2 == function.call(org1));
        
    }
    
    private void assertMatchedAffOrgsKeyByFunction(Function<AffMatchResult, Tuple2<String, String>> function) throws Exception {
        
        // given
        
        AffMatchResult result = new AffMatchResult(new AffMatchAffiliation("DOC_ID", 3), new AffMatchOrganization("ORG_ID"), 0.4f);
        
        // execute
        
        Tuple2<String, String> retKey = function.call(result);
        
        // assert
        
        assertEquals("DOC_ID###3", retKey._1);
        assertEquals("ORG_ID", retKey._2);
    }

    
    private void assertAllAffMatchResultsReduceFunction(Function2<AffMatchResult, AffMatchResult, AffMatchResult> function) throws Exception {
        
        // given
        
        AffMatchResult affMatchResult1 = mock(AffMatchResult.class);
        AffMatchResult affMatchResult2 = mock(AffMatchResult.class);
        
        when(affMatchResultChooser.chooseBetter(affMatchResult1, affMatchResult2)).thenReturn(affMatchResult2);
        
        // assert & assert
        
        assertTrue(affMatchResult2 == function.call(affMatchResult1, affMatchResult2));
    }

}
