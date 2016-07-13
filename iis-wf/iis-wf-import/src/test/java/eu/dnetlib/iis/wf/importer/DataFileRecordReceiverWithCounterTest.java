package eu.dnetlib.iis.wf.importer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Collections;

import org.apache.avro.file.DataFileWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.importer.schemas.Concept;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class DataFileRecordReceiverWithCounterTest {

    private DataFileRecordReceiverWithCounter<Concept> recordReceiver;
    
    @Mock
    private DataFileWriter<Concept> dataFileWriter;
    
    
    @Before
    public void setup() {
        recordReceiver = new DataFileRecordReceiverWithCounter<>(dataFileWriter);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void receive() throws IOException {
        
        // given
        
        Concept concept1 = Concept.newBuilder().setId("ID_1").setLabel("LABEL_1").setParams(Collections.emptyMap()).build();
        Concept concept2 = Concept.newBuilder().setId("ID_2").setLabel("LABEL_2").setParams(Collections.emptyMap()).build();
        
        
        // execute
        
        recordReceiver.receive(concept1);
        recordReceiver.receive(concept2);
        
        
        // assert
        
        verify(dataFileWriter).append(concept1);
        verify(dataFileWriter).append(concept2);
        
        assertEquals(2L, recordReceiver.getReceivedCount());
        
    }
    
    
}
