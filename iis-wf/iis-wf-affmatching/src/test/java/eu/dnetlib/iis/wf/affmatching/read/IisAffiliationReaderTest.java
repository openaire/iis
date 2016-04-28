package eu.dnetlib.iis.wf.affmatching.read;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
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
* @author Łukasz Dumiszewski
*/

@RunWith(MockitoJUnitRunner.class)
public class IisAffiliationReaderTest {

    
    @InjectMocks
    private IisAffiliationReader reader = new IisAffiliationReader();
    
    @Mock
    private AffiliationConverter affiliationConverter;
    
    @Mock
    private SparkAvroLoader sparkAvroLoader;
    
    @Mock
    private JavaSparkContext sparkContext;

    @Mock
    private JavaRDD<ExtractedDocumentMetadata> inputDocuments;

    @Mock
    private JavaRDD<AffMatchAffiliation> affMatchAffiliations;

    
    @Captor
    private ArgumentCaptor<FlatMapFunction<ExtractedDocumentMetadata, AffMatchAffiliation>> convertFunction;

    
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void readAffiliations_sparkContext_null() {
        
        // execute
        
        reader.readAffiliations(null, "/aaa");
        
    }
    

    @Test(expected = IllegalArgumentException.class)
    public void readAffiliations_inputPath_blank() {
        
        // execute
        
        reader.readAffiliations(sparkContext, "  ");
        
    }
    
    
    @Test
    public void readAffiliations() throws Exception {
        
        // given
        
        String inputPath = "/data/affiliations";
        
        
        when(sparkAvroLoader.loadJavaRDD(sparkContext, inputPath, ExtractedDocumentMetadata.class)).thenReturn(inputDocuments);
        
        doReturn(affMatchAffiliations).when(inputDocuments).flatMap(any());

        
        // execute
        
        JavaRDD<AffMatchAffiliation> retAffMatchAffiliations = reader.readAffiliations(sparkContext, inputPath);
        
        
        // assert
        
        assertTrue(affMatchAffiliations == retAffMatchAffiliations);
        
        verify(inputDocuments).flatMap(convertFunction.capture());
        assertConvertFunction(convertFunction.getValue());
    }
    
    
    //------------------------ TESTS --------------------------

    private void assertConvertFunction(FlatMapFunction<ExtractedDocumentMetadata, AffMatchAffiliation> function) throws Exception {

        // given
        
        ExtractedDocumentMetadata doc = new ExtractedDocumentMetadata();
        doc.setId("DOC1");
        
        AffMatchAffiliation affMatchAff1 = new AffMatchAffiliation("DOC1", 1);
        AffMatchAffiliation affMatchAff2 = new AffMatchAffiliation("DOC1", 2);
        
        when(affiliationConverter.convert(doc)).thenReturn(Lists.newArrayList(affMatchAff1, affMatchAff2));

        
        // execute
        
        List<AffMatchAffiliation> affs = Lists.newArrayList(function.call(doc));

        
        // assert
        
        assertNotNull(affs);
        assertEquals(2, affs.size());
        assertTrue(affs.contains(affMatchAff1));
        assertTrue(affs.contains(affMatchAff2));
        
    }
}
