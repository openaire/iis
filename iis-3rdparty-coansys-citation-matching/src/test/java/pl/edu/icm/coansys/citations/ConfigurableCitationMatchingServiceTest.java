package pl.edu.icm.coansys.citations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import static org.mockito.Mockito.mock;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Lists;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import pl.edu.icm.coansys.citations.data.IdWithSimilarity;
import pl.edu.icm.coansys.citations.data.MatchableEntity;

/**
 * @author madryk
 */
public class ConfigurableCitationMatchingServiceTest {

    private ConfigurableCitationMatchingService<String, MatchableEntity, String, MatchableEntity, MatchableEntity, String> citationMatchingService = new ConfigurableCitationMatchingService<>();
    
    @Mock
    private InputCitationReader<String, MatchableEntity> inputCitationReader;
    @Mock
    private InputCitationConverter<String, MatchableEntity> inputCitationConverter;
    
    @Mock
    private InputDocumentReader<String, MatchableEntity> inputDocumentReader;
    @Mock
    private InputDocumentConverter<String, MatchableEntity> inputDocumentConverter;
    
    @Mock
    private OutputConverter<MatchableEntity, String> outputConverter;
    @Mock
    private OutputWriter<MatchableEntity, String> outputWriter;
    
    @Mock
    private CoreCitationMatchingService coreCitationMatchingService;
    
    @Mock
    private JavaSparkContext sparkContext;
    
    @Mock
    private JavaPairRDD<String, MatchableEntity> citations;
    @Mock
    private JavaPairRDD<String, MatchableEntity> convertedCitations;
    @Mock
    private JavaPairRDD<String, MatchableEntity> repartitionedCitations;
    @Captor
    private ArgumentCaptor<Partitioner> citationsPartitioner;
    
    @Mock
    private JavaPairRDD<String, MatchableEntity> documents;
    @Mock
    private JavaPairRDD<String, MatchableEntity> convertedDocuments;
    @Mock
    private JavaPairRDD<String, MatchableEntity> repartitionedDocuments;
    @Captor
    private ArgumentCaptor<Partitioner> documentsPartitioner;
    
    @Mock
    private JavaPairRDD<MatchableEntity, IdWithSimilarity> matched;
    @Mock
    private JavaPairRDD<MatchableEntity, String> convertedMatched;
    
    
    @BeforeEach
    public void setup() {
        MockitoAnnotations.initMocks(this);
        
        citationMatchingService.setInputCitationReader(inputCitationReader);
        citationMatchingService.setInputCitationConverter(inputCitationConverter);
        
        citationMatchingService.setInputDocumentReader(inputDocumentReader);
        citationMatchingService.setInputDocumentConverter(inputDocumentConverter);
        
        citationMatchingService.setOutputConverter(outputConverter);
        citationMatchingService.setOutputWriter(outputWriter);
        
        citationMatchingService.setNumberOfPartitions(5);
        
        citationMatchingService.setCoreCitationMatchingService(coreCitationMatchingService);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void matchCitations() {
        
        // given
        
        when(inputCitationReader.readCitations(sparkContext, "/input/cit/path")).thenReturn(citations);
        when(inputCitationConverter.convertCitations(citations)).thenReturn(convertedCitations);
        Partition[] mock16Partitions = new Partition[16];  // Create an array of 16 Partition objects
        for (int i = 0; i < 16; i++) {
            mock16Partitions[i] = mock(Partition.class);  // Mock each Partition
        }
        when(convertedCitations.partitions()).thenReturn(Arrays.asList(mock16Partitions));
        when(convertedCitations.partitionBy(any())).thenReturn(repartitionedCitations);
        
        when(inputDocumentReader.readDocuments(sparkContext, "/input/doc/path")).thenReturn(documents);
        when(inputDocumentConverter.convertDocuments(documents)).thenReturn(convertedDocuments);
        
        Partition[] mock12Partitions = new Partition[12];  // Create an array of 16 Partition objects
        for (int i = 0; i < 12; i++) {
            mock12Partitions[i] = mock(Partition.class);  // Mock each Partition
        }
        when(convertedDocuments.partitions()).thenReturn(Arrays.asList(mock12Partitions));
        when(convertedDocuments.partitionBy(any())).thenReturn(repartitionedDocuments);
        
        when(coreCitationMatchingService.matchCitations(repartitionedCitations, repartitionedDocuments)).thenReturn(matched);
        when(outputConverter.convertMatchedCitations(matched)).thenReturn(convertedMatched);
        
        
        // execute
        
        citationMatchingService.matchCitations(sparkContext, "/input/cit/path", "/input/doc/path", "/output/path");
        
        
        // assert
        
        verify(inputCitationReader).readCitations(sparkContext, "/input/cit/path");
        verify(inputCitationConverter).convertCitations(citations);
        verify(convertedCitations).partitionBy(citationsPartitioner.capture());
        assertPartitioner(citationsPartitioner.getValue(), 5);
        
        
        verify(inputDocumentReader).readDocuments(sparkContext, "/input/doc/path");
        verify(inputDocumentConverter).convertDocuments(documents);
        verify(convertedDocuments).partitionBy(documentsPartitioner.capture());
        assertPartitioner(documentsPartitioner.getValue(), 5);
        
        verify(coreCitationMatchingService).matchCitations(repartitionedCitations, repartitionedDocuments);
        
        verify(outputConverter).convertMatchedCitations(matched);
        verify(outputWriter).writeMatchedCitations(convertedMatched, "/output/path");
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void assertPartitioner(Partitioner actualPartitioner, int expectedNumPartitions) {
        
        assertTrue(actualPartitioner instanceof HashPartitioner);
        assertEquals(expectedNumPartitions, actualPartitioner.numPartitions());
        
    }
}
