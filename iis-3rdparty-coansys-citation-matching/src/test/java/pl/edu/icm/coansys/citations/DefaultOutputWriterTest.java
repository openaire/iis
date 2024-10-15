package pl.edu.icm.coansys.citations;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import pl.edu.icm.coansys.citations.data.IdWithSimilarity;
import pl.edu.icm.coansys.citations.data.MatchableEntity;
import pl.edu.icm.coansys.citations.data.TextWithBytesWritable;
import scala.Tuple2;

/**
 * @author madryk
 */
public class DefaultOutputWriterTest {

    private DefaultOutputWriter outputWriter = new DefaultOutputWriter();
    
    @Mock
    private JavaPairRDD<MatchableEntity, IdWithSimilarity> matchedCitations;
    
    @Mock
    private JavaPairRDD<TextWithBytesWritable, Text> mappedMatchedCitations;
    
    @Captor
    private ArgumentCaptor<PairFunction<Tuple2<MatchableEntity, IdWithSimilarity>, TextWithBytesWritable, Text>> mapToWritableFunction;
    
    
    @BeforeEach
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void writeMatchedCitations() throws Exception {
        
        // given
        
        doReturn(mappedMatchedCitations).when(matchedCitations).mapToPair(any());
        
        
        // execute
        
        outputWriter.writeMatchedCitations(matchedCitations, "/output/path");
        
        
        // assert
        
        verify(matchedCitations).mapToPair(mapToWritableFunction.capture());
        assertMapToWritableFunction(mapToWritableFunction.getValue());
        
        verify(mappedMatchedCitations).saveAsNewAPIHadoopFile("/output/path", TextWithBytesWritable.class,
                Text.class, SequenceFileOutputFormat.class);
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void assertMapToWritableFunction(PairFunction<Tuple2<MatchableEntity, IdWithSimilarity>, TextWithBytesWritable, Text> function) throws Exception {
        
        MatchableEntity entity = MatchableEntity.fromParameters("some_cit_id", "John Doe", null, "Some Title", null, null, null);
        IdWithSimilarity idWithSimilarity = new IdWithSimilarity("some_doc_id", 0.345);
        Tuple2<MatchableEntity, IdWithSimilarity> matchedCitation = new Tuple2<>(entity, idWithSimilarity);
        
        
        Tuple2<TextWithBytesWritable, Text> matchedCitationWritable = function.call(matchedCitation);
        
        
        assertEquals(matchedCitationWritable._1.text().toString(), "some_cit_id");
        assertArrayEquals(matchedCitationWritable._1.bytes().copyBytes(), entity.data().toByteArray());
        
        assertEquals(matchedCitationWritable._2.toString(), "0.345:some_doc_id");
    }
}
