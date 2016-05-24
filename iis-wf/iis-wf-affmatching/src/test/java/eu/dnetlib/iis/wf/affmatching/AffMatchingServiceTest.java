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
import org.apache.spark.api.java.function.PairFunction;
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

import eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcher;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import eu.dnetlib.iis.wf.affmatching.normalize.AffMatchAffiliationNormalizer;
import eu.dnetlib.iis.wf.affmatching.normalize.AffMatchOrganizationNormalizer;
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
    private AffOrgMatcher affOrgMatcher1;
    
    @Mock
    private AffOrgMatcher affOrgMatcher2;
    
    @Mock
    private AffMatchResultWriter affMatchResultWriter;

    
    
    //----------------------------------- DATA -------------------------------------------

    @Mock
    private JavaSparkContext sc;
    
    private String inputAffPath = "/input/affs";
    
    private String inputOrgPath = "/input/orgs";
    
    private String outputPath = "/output";
    
    
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
    private JavaPairRDD<String, AffMatchAffiliation> idAffiliations;
    
    @Mock
    private JavaRDD<AffMatchResult> parallelizedAffMatchResults;
    
    @Mock
    private JavaRDD<AffMatchResult> matchedAffOrgs1;

    @Mock
    private JavaRDD<AffMatchResult> matchedAffOrgs2;
    
    @Mock
    private JavaRDD<AffMatchAffiliation> idAffiliationValues;

    @Mock
    private JavaPairRDD<String, AffMatchAffiliation> idAffiliations1;

    @Mock
    private JavaRDD<AffMatchAffiliation> idAffiliation1Values;

    @Mock
    private JavaPairRDD<String, AffMatchAffiliation> idAffiliations2;
    
    @Mock
    private JavaRDD<AffMatchResult> allAffMatchResults1;
    
    @Mock
    private JavaRDD<AffMatchResult> allAffMatchResults2;
    
    @Mock
    private JavaPairRDD<String, String> matchedAffIds1;
    
    @Mock
    private JavaPairRDD<String, String> matchedAffIds2;
    
    
    
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
    private ArgumentCaptor<Function<AffMatchAffiliation, String>> affKeyByFunction;
    
    @Captor
    private ArgumentCaptor<PairFunction<AffMatchResult, String, String>> matchedAffIdFunction;
    
    
    
    @Before
    public void before() {
        
        affMatchingService.setAffOrgMatchers(Lists.newArrayList(affOrgMatcher1, affOrgMatcher2));
        
        
    }
    
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test(expected = NullPointerException.class)
    public void matchAffiliations_sparkContext_null() {
        
        // execute
        
        affMatchingService.matchAffiliations(null, inputAffPath, inputOrgPath, outputPath);
        
    }
    

    @Test(expected = IllegalArgumentException.class)
    public void matchAffiliations_inputAffPath_blank() {
        
        // execute
        
        affMatchingService.matchAffiliations(sc, "  ", inputOrgPath, outputPath);
        
    }


    @Test(expected = IllegalArgumentException.class)
    public void matchAffiliations_inputOrgPath_blank() {
        
        // execute
        
        affMatchingService.matchAffiliations(sc, inputAffPath, " ", outputPath);
        
    }


    @Test(expected = IllegalArgumentException.class)
    public void matchAffiliations_outputPath_blank() {
        
        // execute
        
        affMatchingService.matchAffiliations(sc, inputAffPath, inputOrgPath, "   ");
        
    }


    @Test(expected = NullPointerException.class)
    public void matchAffiliations_affiliationReader_null() {
        
        // given
        
        affMatchingService.setAffiliationReader(null);
        
        
        // execute
        
        affMatchingService.matchAffiliations(sc, inputAffPath, inputOrgPath, outputPath);
        
    }

    
    @Test(expected = NullPointerException.class)
    public void matchAffiliations_organizationReader_null() {
        
        // given
        
        affMatchingService.setOrganizationReader(null);
        
        
        // execute
        
        affMatchingService.matchAffiliations(sc, inputAffPath, inputOrgPath, outputPath);
        
    }
    

    @Test(expected = NullPointerException.class)
    public void matchAffiliations_affMatchResultWriter_null() {
        
        // given
        
        affMatchingService.setAffMatchResultWriter(null);
        
        
        // execute
        
        affMatchingService.matchAffiliations(sc, inputAffPath, inputOrgPath, outputPath);
        
    }

    
    @Test(expected = IllegalStateException.class)
    public void matchAffiliations_affOrgMatchers_empty() {
        
        // given
        
        affMatchingService.setAffOrgMatchers(Lists.newArrayList());
        
        
        // execute
        
        affMatchingService.matchAffiliations(sc, inputAffPath, inputOrgPath, outputPath);
        
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
    
        
        //--- matching
        
        doReturn(idAffiliations).when(normalizedAndFilteredAffiliations).keyBy(Mockito.any());
        doReturn(parallelizedAffMatchResults).when(sc).parallelize(new ArrayList<>());
        
        //- first matcher
        when(idAffiliations.values()).thenReturn(idAffiliationValues);
        when(affOrgMatcher1.match(idAffiliationValues, normalizedAndFilteredOrganizations)).thenReturn(matchedAffOrgs1);
        when(parallelizedAffMatchResults.union(matchedAffOrgs1)).thenReturn(allAffMatchResults1);
        doReturn(matchedAffIds1).when(matchedAffOrgs1).mapToPair(Mockito.any());
        when(idAffiliations.subtractByKey(matchedAffIds1)).thenReturn(idAffiliations1);
        
        //- second matcher
        when(idAffiliations1.values()).thenReturn(idAffiliation1Values);
        when(affOrgMatcher2.match(idAffiliation1Values, normalizedAndFilteredOrganizations)).thenReturn(matchedAffOrgs2);
        when(allAffMatchResults1.union(matchedAffOrgs2)).thenReturn(allAffMatchResults2);
        doReturn(matchedAffIds2).when(matchedAffOrgs2).mapToPair(Mockito.any());
        when(idAffiliations1.subtractByKey(matchedAffIds2)).thenReturn(idAffiliations2);
        
        
        
        // execute
        
        affMatchingService.matchAffiliations(sc, inputAffPath, inputOrgPath, outputPath);
        
        
        // assert
        
        verify(affMatchResultWriter).write(allAffMatchResults2, outputPath);
        
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

        verify(normalizedAndFilteredAffiliations).keyBy(affKeyByFunction.capture());
        assertAffKeyByFunction(affKeyByFunction.getValue());
        
        verify(matchedAffOrgs1).mapToPair(matchedAffIdFunction.capture());
        assertMatchedAffIdFunction(matchedAffIdFunction.getValue());
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

    
    private void assertAffKeyByFunction(Function<AffMatchAffiliation, String> function) throws Exception {
        
        // given
        
        AffMatchAffiliation aff = mock(AffMatchAffiliation.class);
        when(aff.getId()).thenReturn("XYZ");
        
        // execute & assert
        
        assertEquals("XYZ", function.call(aff));
        
    }
    
    
    private void assertMatchedAffIdFunction(PairFunction<AffMatchResult, String, String> function) throws Exception {
        
        // given
        
        AffMatchResult affMatchResult = mock(AffMatchResult.class);
        AffMatchAffiliation aff = mock(AffMatchAffiliation.class);
        
        when(affMatchResult.getAffiliation()).thenReturn(aff);
        when(aff.getId()).thenReturn("XYZ");
        
        
        // execute & assert
        
        assertEquals(new Tuple2<>("XYZ", ""), function.call(affMatchResult));
        
    }

}
