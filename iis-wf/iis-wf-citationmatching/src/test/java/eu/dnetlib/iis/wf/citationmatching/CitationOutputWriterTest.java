package eu.dnetlib.iis.wf.citationmatching;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.citationmatching.schemas.Citation;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class CitationOutputWriterTest {

    @InjectMocks
    private CitationOutputWriter citationOutputWriter = new CitationOutputWriter();
    
    @Mock
    private SparkAvroSaver avroSaver;
    
    @Mock
    private CitationMatchingCounterReporter citationMatchingReporter;
    
    @Mock
    private JavaPairRDD<Citation, NullWritable> matchedCitations;
    
    @Mock
    private JavaRDD<Citation> matchedCitationsKeys;
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void writeMatchedCitations() {
        
        // given
        
        String path = "/matched/citations/path";
        
        when(matchedCitations.keys()).thenReturn(matchedCitationsKeys);
        
        
        // execute
        
        citationOutputWriter.writeMatchedCitations(matchedCitations, path);
        
        
        // assert
        
        verify(matchedCitationsKeys).cache();
        verify(avroSaver).saveJavaRDD(matchedCitationsKeys, Citation.SCHEMA$, path);
        verify(citationMatchingReporter).report(matchedCitationsKeys);
        
    }
    
}
