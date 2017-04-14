package eu.dnetlib.iis.wf.importer.content.approver;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;



/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class ComplexContentApproverTest {

    @Mock
    private ContentApprover approver1;
    
    @Mock
    private ContentApprover approver2;
    
    @Captor
    private ArgumentCaptor<byte[]> approver1Captor;
    
    @Captor
    private ArgumentCaptor<byte[]> approver2Captor;
    
    
    private final byte[] content = "content".getBytes();
    
    // ----------------------------- TESTS ----------------------------
    
    @Test
    public void testApproveEmpty() throws Exception {
        // given
        ComplexContentApprover complexApprover = new ComplexContentApprover();
        
        // execute & assert
        assertTrue(complexApprover.approve(content));
    }

    @Test
    public void testApprove() throws Exception {
        // given
        ComplexContentApprover complexApprover = new ComplexContentApprover(approver1, approver2);
        doReturn(true).when(approver1).approve(content);
        doReturn(true).when(approver2).approve(content);
        
        // execute
        boolean result = complexApprover.approve(content);
        
        // assert
        assertTrue(result);
        verify(approver1).approve(approver1Captor.capture());
        verify(approver2).approve(approver2Captor.capture());
        assertTrue(content == approver1Captor.getValue());
        assertTrue(content == approver2Captor.getValue());
    }
    
    @Test
    public void testDisapproveFirst() throws Exception {
     // given
        ComplexContentApprover complexApprover = new ComplexContentApprover(approver1, approver2);
        doReturn(false).when(approver1).approve(content);
        
        // execute
        boolean result = complexApprover.approve(content);
        
        // assert
        assertFalse(result);
        verify(approver1, times(1)).approve(approver1Captor.capture());
        verify(approver2, never()).approve(approver2Captor.capture());
        assertTrue(content == approver1Captor.getValue());
    }

    @Test
    public void testDisapproveLast() throws Exception {
        // given
        ComplexContentApprover complexApprover = new ComplexContentApprover(approver1, approver2);
        doReturn(true).when(approver1).approve(content);
        doReturn(false).when(approver2).approve(content);
        
        // execute
        boolean result = complexApprover.approve(content);
        
        // assert
        assertFalse(result);
        verify(approver1).approve(approver1Captor.capture());
        verify(approver2).approve(approver2Captor.capture());
        assertTrue(content == approver1Captor.getValue());
        assertTrue(content == approver2Captor.getValue());
    }
    
}

