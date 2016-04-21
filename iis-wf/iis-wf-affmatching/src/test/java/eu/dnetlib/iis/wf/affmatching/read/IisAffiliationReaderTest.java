package eu.dnetlib.iis.wf.affmatching.read;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class IisAffiliationReaderTest {

    @InjectMocks
    private IisAffiliationReader affiliationReader = new IisAffiliationReader();
    
    @Mock
    private SparkAvroLoader avroLoader = new SparkAvroLoader();
    
    @Mock
    private AffiliationConverter affiliationConverter = new AffiliationConverter();
    
    
    @Mock
    private JavaSparkContext sparkContext;
    
    
    @Mock
    private JavaRDD<ExtractedDocumentMetadata> loadedDocumentMetadata;
    
    @Captor
    private ArgumentCaptor<FlatMapFunction<ExtractedDocumentMetadata, AffMatchAffiliation>> extractAffiliationsFunction;
    
    @Mock
    private JavaRDD<AffMatchAffiliation> affiliations;
    
    
    @Before
    public void setUp() {
        
        doReturn(loadedDocumentMetadata).when(avroLoader).loadJavaRDD(sparkContext, "/path/to/affiliations/", ExtractedDocumentMetadata.class);
        doReturn(affiliations).when(loadedDocumentMetadata).flatMap(any());
        
    }
    
    //------------------------ TESTS --------------------------

    @Test(expected = NullPointerException.class)
    public void readAffiliations_NULL_CONTEXT() {
        
        // execute
        affiliationReader.readAffiliations(null, "/path/to/affiliations/");
        
    }
    
    
    @Test(expected = NullPointerException.class)
    public void readAffiliations_NULL_PATH() {
        
        // execute
        affiliationReader.readAffiliations(sparkContext, null);
        
    }
    
    
    @Test
    public void readAffiliations() throws Exception {
        
        // execute
        
        JavaRDD<AffMatchAffiliation> retAffiliations = affiliationReader.readAffiliations(sparkContext, "/path/to/affiliations/");
        
        // assert
        
        assertTrue(retAffiliations == affiliations);
        
        verify(avroLoader).loadJavaRDD(sparkContext, "/path/to/affiliations/", ExtractedDocumentMetadata.class);
        
        verify(loadedDocumentMetadata).flatMap(extractAffiliationsFunction.capture());
        assertExtractAffiliationsFunction(extractAffiliationsFunction.getValue());
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    public void assertExtractAffiliationsFunction(FlatMapFunction<ExtractedDocumentMetadata, AffMatchAffiliation> function) throws Exception {
        
        AffMatchAffiliation firstAff = mock(AffMatchAffiliation.class);
        AffMatchAffiliation secondAff = mock(AffMatchAffiliation.class);
        ExtractedDocumentMetadata doc = mock(ExtractedDocumentMetadata.class);
        
        when(affiliationConverter.convert(doc)).thenReturn(Lists.newArrayList(firstAff, secondAff));
        
        
        Iterable<AffMatchAffiliation> retAffiliations = function.call(doc);
        
        
        List<AffMatchAffiliation> retAffiliationList = Lists.newArrayList(retAffiliations);
        
        assertTrue(retAffiliationList.get(0) == firstAff);
        assertTrue(retAffiliationList.get(1) == secondAff);
        
    }
    
}
