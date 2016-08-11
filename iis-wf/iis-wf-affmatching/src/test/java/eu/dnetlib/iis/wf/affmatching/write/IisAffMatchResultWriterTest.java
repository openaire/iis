package eu.dnetlib.iis.wf.affmatching.write;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
* @author Łukasz Dumiszewski
*/

@RunWith(MockitoJUnitRunner.class)
public class IisAffMatchResultWriterTest {

    
    @InjectMocks
    private IisAffMatchResultWriter writer = new IisAffMatchResultWriter();
    
    
    // SERVICES
    
    @Mock
    private AffMatchResultConverter affMatchResultConverter;
    
    @Mock
    private DuplicateMatchedOrgStrengthRecalculator duplicateMatchedOrgStrengthRecalculator;
    
    @Mock
    private SparkAvroSaver sparkAvroSaver;
    
    @Mock
    private AffMatchReportGenerator reportGenerator;
    
    
    // DATA
    
    @Mock
    private JavaRDD<AffMatchResult> affMatchResults;
    
    @Mock
    private JavaRDD<MatchedOrganization> matchedOrganizations;
    
    @Mock
    private JavaPairRDD<Tuple2<CharSequence, CharSequence>, MatchedOrganization> matchedOrganizationsDocOrgIdKey;
    
    @Mock
    private JavaPairRDD<Tuple2<CharSequence, CharSequence>, MatchedOrganization> distinctMatchedOrganizations;
    
    @Mock
    private JavaRDD<MatchedOrganization> distinctMatchedOrganizationsValues;
    
    @Mock
    private List<ReportEntry> reportEntries;
    
    @Mock
    private JavaRDD<ReportEntry> rddReportEntries;
    
    @Mock
    private JavaSparkContext sc;
    
    
    // FUNCTIONS CAPTORS
    
    @Captor
    private ArgumentCaptor<Function<AffMatchResult, MatchedOrganization>> convertFunction;
    
    @Captor
    private ArgumentCaptor<Function<MatchedOrganization, Tuple2<CharSequence, CharSequence>>> extractDocOrgIdFunction;
    
    @Captor
    private ArgumentCaptor<Function2<MatchedOrganization, MatchedOrganization, MatchedOrganization>> duplicateMatchedOrgsReduceFunction;
    
    
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test(expected = NullPointerException.class)
    public void write_sc_null() {
        
        // execute
        
        writer.write(null, affMatchResults, "/output", "/report");
        
    }
    
    @Test(expected = NullPointerException.class)
    public void write_matchedAffOrgs_null() {
        
        // execute
        
        writer.write(sc, null, "/output", "/report");
        
    }
    

    @Test(expected = IllegalArgumentException.class)
    public void write_outputPath_blank() {
        
        // execute
        
        writer.write(sc, affMatchResults, "  ", "/report");
        
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void write_outputReportPath_blank() {
        
        // execute
        
        writer.write(sc, affMatchResults, "/output", " ");
        
    }
    
    @Test
    public void write() throws Exception {
        
        // given
        
        String outputPath = "/data/matchedAffiliations/output";
        String outputReportPath = "/data/matchedAffiliations/report";
        
        doReturn(matchedOrganizations).when(affMatchResults).map(any());
        doReturn(matchedOrganizationsDocOrgIdKey).when(matchedOrganizations).keyBy(any());
        when(matchedOrganizationsDocOrgIdKey.reduceByKey(any())).thenReturn(distinctMatchedOrganizations);
        when(distinctMatchedOrganizations.values()).thenReturn(distinctMatchedOrganizationsValues);
        when(reportGenerator.generateReport(distinctMatchedOrganizationsValues)).thenReturn(reportEntries);
        when(sc.parallelize(reportEntries)).thenReturn(rddReportEntries);
        
        
        // execute
        
        writer.write(sc, affMatchResults, outputPath, outputReportPath);
        
        
        // assert
        
        verify(sparkAvroSaver).saveJavaRDD(distinctMatchedOrganizationsValues, MatchedOrganization.SCHEMA$, outputPath);
        verify(sparkAvroSaver).saveJavaRDD(rddReportEntries, ReportEntry.SCHEMA$, outputReportPath);
        
        verify(affMatchResults).map(convertFunction.capture());
        assertConvertFunction(convertFunction.getValue());
        
        verify(matchedOrganizations).keyBy(extractDocOrgIdFunction.capture());
        assertExtractDocOrgIdFunction(extractDocOrgIdFunction.getValue());
        
        verify(matchedOrganizationsDocOrgIdKey).reduceByKey(duplicateMatchedOrgsReduceFunction.capture());
        assertDuplicateMatchedOrgsReduceFunction(duplicateMatchedOrgsReduceFunction.getValue());
        
        verify(distinctMatchedOrganizations).values();
    }
    
    
    
    //------------------------ PRIVATE --------------------------

    
    private void assertConvertFunction(Function<AffMatchResult, MatchedOrganization> function) throws Exception {

        // given
        
        AffMatchResult affMatchResult = mock(AffMatchResult.class);
        MatchedOrganization matchedAff = mock(MatchedOrganization.class);
        
        when(affMatchResultConverter.convert(affMatchResult)).thenReturn(matchedAff);

        
        // execute
        
        MatchedOrganization retMatchedAff = function.call(affMatchResult);

        
        // assert
        
        assertNotNull(retMatchedAff);
        assertTrue(matchedAff == retMatchedAff);
        
    }
    
    private void assertExtractDocOrgIdFunction(Function<MatchedOrganization, Tuple2<CharSequence, CharSequence>> function) throws Exception {
        
        // given
        MatchedOrganization matchedOrg = new MatchedOrganization("DOC_ID", "ORG_ID", 0.6f);
        
        // execute
        Tuple2<CharSequence, CharSequence> extractedDocOrgId = function.call(matchedOrg);
        
        // assert
        assertEquals("DOC_ID", extractedDocOrgId._1);
        assertEquals("ORG_ID", extractedDocOrgId._2);
    }
    
    private void assertDuplicateMatchedOrgsReduceFunction(Function2<MatchedOrganization, MatchedOrganization, MatchedOrganization> function) throws Exception {
        
        // given
        
        MatchedOrganization matchedOrg1 = mock(MatchedOrganization.class);
        MatchedOrganization matchedOrg2 = mock(MatchedOrganization.class);
        MatchedOrganization newMatchedOrg = mock(MatchedOrganization.class);
        
        when(duplicateMatchedOrgStrengthRecalculator.recalculateStrength(matchedOrg1, matchedOrg2)).thenReturn(newMatchedOrg);
        
        
        // execute
        
        MatchedOrganization retMatchedOrg = function.call(matchedOrg1, matchedOrg2);
        
        
        // assert
        
        assertNotNull(retMatchedOrg);
        assertTrue(retMatchedOrg == newMatchedOrg);
    }
}
