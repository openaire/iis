package eu.dnetlib.iis.wf.export.actionmanager.entity;

import static org.junit.Assert.assertEquals;
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
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import scala.Tuple2;

/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class DocumentFilterTest  {
    
    @InjectMocks
    private DocumentFilter entityFilter = new DocumentFilter();
    
    @Mock
    private SparkAvroLoader sparkAvroLoader;
    
    @Mock
    private JavaSparkContext sparkContext;
    
    @Mock
    private JavaRDD<DocumentText> inputEntities;

    @Mock
    private JavaRDD<DocumentToProject> inputRelations;
    
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
    private ArgumentCaptor<PairFunction<DocumentToProject, CharSequence, Object>> relationMapToPairCaptor;
    
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
        when(sparkAvroLoader.loadJavaRDD(sparkContext, relationsInputPath, DocumentToProject.class)).thenReturn(inputRelations);
        doReturn(relationIdToBlankDuplicated).when(inputRelations).mapToPair(any());
        doReturn(relationIdToBlank).when(relationIdToBlankDuplicated).distinct();
        doReturn(entityIdToText).when(inputEntities).mapToPair(any());
        doReturn(joinedById).when(relationIdToBlank).join(entityIdToText);
        doReturn(expectedOutput).when(joinedById).map(any());
        
        // execute
        JavaRDD<CharSequence> output = entityFilter.provideRDD(sparkContext, relationsInputPath, entitiesInputPath);
        
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
    // ------------------------ PRIVATE --------------------------
    
    private void assertRelationMapToPair(PairFunction<DocumentToProject, CharSequence, Object> function) throws Exception {
        String docId = "docId";
        String projId = "projId";
        
        Tuple2<CharSequence, Object> result = function.call(DocumentToProject.newBuilder().setDocumentId(docId)
                .setProjectId(projId).setConfidenceLevel(1).build());
        
        assertEquals(docId, result._1);
        assertNull(result._2);
    }
    
    private void assertEntityMapToPair(PairFunction<DocumentText, CharSequence, CharSequence> function) throws Exception {
        String id = "docId";
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
