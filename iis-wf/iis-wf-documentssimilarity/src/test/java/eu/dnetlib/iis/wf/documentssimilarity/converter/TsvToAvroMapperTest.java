package eu.dnetlib.iis.wf.documentssimilarity.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import eu.dnetlib.iis.documentssimilarity.schemas.DocumentSimilarity;

/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class TsvToAvroMapperTest {

    @Mock
    private Context context;
    
    @Mock
    private Logger log;
    
    @Captor
    private ArgumentCaptor<AvroKey<DocumentSimilarity>> keyCaptor;
    
    @Captor
    private ArgumentCaptor<NullWritable> valueCaptor;
    
    
    private TsvToAvroMapper mapper = new TsvToAvroMapper();

    
    @Before
    public void before() {
        Whitebox.setInternalState(TsvToAvroMapper.class, "log", log);
    }
    
    // ------------------------------------- TESTS -----------------------------------

    @Test
    public void testMap() throws Exception {
        // given
        String docId = "doc1";
        String otherDocId = "doc1";
        float similarityLevel = 0.9f;
        
        // execute
        mapper.map(null, new Text(buildLine(docId, otherDocId, similarityLevel)), context);
        
        // assert
        verify(context).write(keyCaptor.capture(), valueCaptor.capture());
        assertTrue(NullWritable.get() == valueCaptor.getValue());
        DocumentSimilarity docSim = keyCaptor.getValue().datum();
        assertNotNull(docSim);
        assertEquals(docId, docSim.getDocumentId());
        assertEquals(otherDocId, docSim.getOtherDocumentId());
        assertEquals(similarityLevel, docSim.getSimilarity().floatValue(), 0.00001);
        verifyZeroInteractions(log);
    }
    
    @Test
    public void testMapForInvalidInput() throws Exception {
     // given
        String input = "invalid line";
        
        // execute
        mapper.map(null, new Text(input), context);
        
        // assert
        verify(context, never()).write(any(), any());
        verify(log).error(isA(ArrayIndexOutOfBoundsException.class));
        
    }
    
    // ------------------------------------- TESTS -----------------------------------
    
    private static String buildLine(String docId, String otherDocId, float similarityLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(docId);
        strBuilder.append('\t');
        strBuilder.append(otherDocId);
        strBuilder.append('\t');
        strBuilder.append(similarityLevel);
        return strBuilder.toString();
    }
}
