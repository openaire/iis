package eu.dnetlib.iis.wf.importer;

import eu.dnetlib.iis.importer.schemas.Concept;
import org.apache.avro.file.DataFileWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

/**
 * @author madryk
 */
@ExtendWith(MockitoExtension.class)
public class DataFileRecordReceiverWithCounterTest {

    private DataFileRecordReceiverWithCounter<Concept> recordReceiver;
    
    @Mock
    private DataFileWriter<Concept> dataFileWriter;
    
    
    @BeforeEach
    public void setup() {
        recordReceiver = new DataFileRecordReceiverWithCounter<>(dataFileWriter);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void receive() throws IOException {
        
        // given
        
        Concept concept1 = Concept.newBuilder().setId("ID_1").setLabel("LABEL_1").setParams(Collections.emptyList()).build();
        Concept concept2 = Concept.newBuilder().setId("ID_2").setLabel("LABEL_2").setParams(Collections.emptyList()).build();
        
        
        // execute
        
        recordReceiver.receive(concept1);
        recordReceiver.receive(concept2);
        
        
        // assert
        
        verify(dataFileWriter).append(concept1);
        verify(dataFileWriter).append(concept2);
        
        assertEquals(2L, recordReceiver.getReceivedCount());
        
    }
    
    
}
