package eu.dnetlib.iis.wf.affmatching.write;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
* @author Łukasz Dumiszewski
*/

@ExtendWith(MockitoExtension.class)
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
    private JavaRDD<MatchedOrganization> distinctMatchedOrganizationsValuesRepartition;
 
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
    
    
    @Test
    public void write_sc_null() {
        
        // execute
        assertThrows(NullPointerException.class, () ->
                writer.write(null, affMatchResults, "/output", "/report", 1));
        
    }
    
    @Test
    public void write_matchedAffOrgs_null() {
        
        // execute
        assertThrows(NullPointerException.class, () ->
                writer.write(sc, null, "/output", "/report", 1));
        
    }
    

    @Test
    public void write_outputPath_blank() {
        
        // execute
        assertThrows(IllegalArgumentException.class, () ->
                writer.write(sc, affMatchResults, "  ", "/report", 1));
        
    }
    
    @Test
    public void write_outputReportPath_blank() {
        
        // execute
        assertThrows(IllegalArgumentException.class, () ->
                writer.write(sc, affMatchResults, "/output", " ", 1));
        
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
        when(distinctMatchedOrganizationsValues.repartition(2)).thenReturn(distinctMatchedOrganizationsValuesRepartition);
        when(reportGenerator.generateReport(distinctMatchedOrganizationsValuesRepartition)).thenReturn(reportEntries);
        when(sc.parallelize(reportEntries, 1)).thenReturn(rddReportEntries);
        
        
        // execute
        
        writer.write(sc, affMatchResults, outputPath, outputReportPath, 2);
        
        
        // assert
        
        verify(sparkAvroSaver).saveJavaRDD(distinctMatchedOrganizationsValuesRepartition, MatchedOrganization.SCHEMA$, outputPath);
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
        assertSame(matchedAff, retMatchedAff);
        
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
        assertSame(retMatchedOrg, newMatchedOrg);
    }
}
