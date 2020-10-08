package eu.dnetlib.iis.wf.importer.content.approver;

import org.apache.hadoop.mapreduce.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

/**
 * @author madryk
 */
@ExtendWith(MockitoExtension.class)
public class InvalidCountableContentApproverWrapperTest {

    private InvalidCountableContentApproverWrapper contentApproverWrapper;
    
    @Mock
    private ContentApprover internalContentApprover;
    
    @Mock
    private Counter invalidContentCounter;
    
    
    @BeforeEach
    public void setup() {
        
        contentApproverWrapper = new InvalidCountableContentApproverWrapper(internalContentApprover, invalidContentCounter);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void approve_VALID() {
        
        // given
        
        byte[] content = new byte[] { };
        when(internalContentApprover.approve(content)).thenReturn(true);
        
        // execute
        
        boolean result = contentApproverWrapper.approve(content);
        
        
        // assert
        
        assertTrue(result);
        verifyZeroInteractions(invalidContentCounter);
    }
    
    @Test
    public void approve_INVALID() {
        
        // given
        
        byte[] content = new byte[] { };
        when(internalContentApprover.approve(content)).thenReturn(false);
        
        // execute
        
        boolean result = contentApproverWrapper.approve(content);
        
        
        // assert
        
        assertFalse(result);
        verify(invalidContentCounter).increment(1L);
    }
}
