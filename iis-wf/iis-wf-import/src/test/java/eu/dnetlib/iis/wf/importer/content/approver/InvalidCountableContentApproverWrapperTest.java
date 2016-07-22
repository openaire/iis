package eu.dnetlib.iis.wf.importer.content.approver;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.apache.hadoop.mapreduce.Counter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.wf.importer.content.approver.ContentApprover;
import eu.dnetlib.iis.wf.importer.content.approver.InvalidCountableContentApproverWrapper;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class InvalidCountableContentApproverWrapperTest {

    private InvalidCountableContentApproverWrapper contentApproverWrapper;
    
    @Mock
    private ContentApprover internalContentApprover;
    
    @Mock
    private Counter invalidContentCounter;
    
    
    @Before
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
