package eu.dnetlib.iis.wf.export.actionmanager.entity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.dataset.schemas.DocumentToDataSet;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import scala.Tuple2;

/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class DatasetFilterTest  {
    
    @InjectMocks
    private DatasetFilter entityFilter = new DatasetFilter();
    
    @Mock
    private SparkAvroLoader sparkAvroLoader;
    
    @Mock
    private JavaSparkContext sparkContext;
    
    @Mock
    private JavaRDD<DocumentText> inputEntities;

    @Mock
    private JavaRDD<DocumentToDataSet> inputRelations;
    
    @Mock
    private JavaRDD<DocumentToDataSet> inputFilteredRelations;
    
    @Mock
    private JavaPairRDD<CharSequence, Object> relationIdToBlankDuplicated;
    
    @Mock
    private JavaPairRDD<CharSequence, Object> relationIdToBlank;
    
    @Mock
    private JavaPairRDD<CharSequence, CharSequence> entityIdToText;
    
    @Mock
    private JavaPairRDD<CharSequence,Tuple2<Object,CharSequence>> joinedById;
    
    @Mock
    private JavaRDD<CharSequence> expectedOutput;
    
    @Captor
    private ArgumentCaptor<PairFunction<DocumentToDataSet, CharSequence, Object>> relationMapToPairCaptor;
    
    @Captor
    private ArgumentCaptor<Function<DocumentToDataSet, Boolean>> relationFilterCaptor;
    
    @Captor
    private ArgumentCaptor<PairFunction<DocumentText, CharSequence, CharSequence>> entityMapToPairCaptor;
    
    @Captor
    private ArgumentCaptor<Function<Tuple2<CharSequence, Tuple2<Object,CharSequence>>,String>> finalMapCaptor;
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void test_execute() throws Exception {
        
        // given
        String entitiesInputPath = "/data/entities";
        String relationsInputPath = "/data/relations";
        
        when(sparkAvroLoader.loadJavaRDD(sparkContext, entitiesInputPath, DocumentText.class)).thenReturn(inputEntities);
        when(sparkAvroLoader.loadJavaRDD(sparkContext, relationsInputPath, DocumentToDataSet.class)).thenReturn(inputRelations);
        doReturn(relationIdToBlankDuplicated).when(inputRelations).mapToPair(any());
        doReturn(relationIdToBlank).when(relationIdToBlankDuplicated).distinct();
        doReturn(entityIdToText).when(inputEntities).mapToPair(any());
        doReturn(joinedById).when(relationIdToBlank).join(entityIdToText);
        doReturn(expectedOutput).when(joinedById).map(any());
        
        // execute
        JavaRDD<CharSequence> output = entityFilter.provideRDD(sparkContext, relationsInputPath, entitiesInputPath, null);
        
        // assert
        assertNotNull(output);
        assertTrue(expectedOutput == output);
        
        verify(inputRelations).mapToPair(relationMapToPairCaptor.capture());
        assertRelationMapToPair(relationMapToPairCaptor.getValue());
        
        verify(inputEntities).mapToPair(entityMapToPairCaptor.capture());
        assertEntityMapToPair(entityMapToPairCaptor.getValue());
        
        verify(joinedById).map(finalMapCaptor.capture());
        assertFinalMap(finalMapCaptor.getValue());
    }
    
    @Test
    public void test_execute_with_filtering() throws Exception {
        
        // given
        String entitiesInputPath = "/data/entities";
        String relationsInputPath = "/data/relations";
        Float trustLevelThreshold = 0.9f;
        
        when(sparkAvroLoader.loadJavaRDD(sparkContext, entitiesInputPath, DocumentText.class)).thenReturn(inputEntities);
        when(sparkAvroLoader.loadJavaRDD(sparkContext, relationsInputPath, DocumentToDataSet.class)).thenReturn(inputRelations);
        doReturn(inputFilteredRelations).when(inputRelations).filter(any());
        doReturn(relationIdToBlankDuplicated).when(inputFilteredRelations).mapToPair(any());
        doReturn(relationIdToBlank).when(relationIdToBlankDuplicated).distinct();
        doReturn(entityIdToText).when(inputEntities).mapToPair(any());
        doReturn(joinedById).when(relationIdToBlank).join(entityIdToText);
        doReturn(expectedOutput).when(joinedById).map(any());
        
        // execute
        JavaRDD<CharSequence> output = entityFilter.provideRDD(sparkContext, relationsInputPath, entitiesInputPath, trustLevelThreshold);
        
        // assert
        assertNotNull(output);
        assertTrue(expectedOutput == output);
        
        verify(inputRelations).filter(relationFilterCaptor.capture());
        assertRelationFilter(relationFilterCaptor.getValue());
        
        verify(inputFilteredRelations).mapToPair(relationMapToPairCaptor.capture());
        assertRelationMapToPair(relationMapToPairCaptor.getValue());
        
        verify(inputEntities).mapToPair(entityMapToPairCaptor.capture());
        assertEntityMapToPair(entityMapToPairCaptor.getValue());
        
        verify(joinedById).map(finalMapCaptor.capture());
        assertFinalMap(finalMapCaptor.getValue());
    }
    
    // ------------------------ PRIVATE --------------------------
    
    private void assertRelationFilter(Function<DocumentToDataSet, Boolean> function) throws Exception {
        String docId = "docId";
        String datasetId = "datasetId";
        
        assertTrue(function.call(DocumentToDataSet.newBuilder().setDocumentId(docId)
                .setDatasetId(datasetId).setConfidenceLevel(1).build()));
        
        assertFalse(function.call(DocumentToDataSet.newBuilder().setDocumentId(docId)
                .setDatasetId(datasetId).setConfidenceLevel(0.5f).build()));
    }
    
    private void assertRelationMapToPair(PairFunction<DocumentToDataSet, CharSequence, Object> function) throws Exception {
        String docId = "docId";
        String datasetId = "datasetId";
        
        Tuple2<CharSequence, Object> result = function.call(DocumentToDataSet.newBuilder().setDocumentId(docId)
                .setDatasetId(datasetId).setConfidenceLevel(1).build());
        
        assertEquals(datasetId, result._1);
        assertNull(result._2);
    }
    
    private void assertEntityMapToPair(PairFunction<DocumentText, CharSequence, CharSequence> function) throws Exception {
        String id = "datasetId";
        String text = "some text";
        
        Tuple2<CharSequence, CharSequence> result = function
                .call(DocumentText.newBuilder().setId(id).setText(text).build());
        
        assertEquals(id, result._1);
        assertEquals(text, result._2);
    }
    
    private void assertFinalMap(Function<Tuple2<CharSequence, Tuple2<Object,CharSequence>>,String> function) throws Exception {
        String entityText = "entity text";
        String joinedId = "some id";
        
        Tuple2<CharSequence, Tuple2<Object,CharSequence>> joinedById = new Tuple2<CharSequence, Tuple2<Object,CharSequence>>(
                joinedId, new Tuple2<Object,CharSequence>(null, entityText));
        
        String outputText = function.call(joinedById);
        
        assertEquals(entityText, outputText);
    }
}
