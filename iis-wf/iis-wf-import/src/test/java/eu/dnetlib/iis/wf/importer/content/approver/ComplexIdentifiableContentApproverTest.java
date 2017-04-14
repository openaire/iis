package eu.dnetlib.iis.wf.importer.content.approver;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
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
public class ComplexIdentifiableContentApproverTest {

    @Mock
    private IdentifiableContentApprover approver1;
    
    @Mock
    private IdentifiableContentApprover approver2;
    
    @Captor
    private ArgumentCaptor<byte[]> approver1ContentCaptor;
    
    @Captor
    private ArgumentCaptor<byte[]> approver2ContentCaptor;
    
    @Captor
    private ArgumentCaptor<String> approver1IdCaptor;
    
    @Captor
    private ArgumentCaptor<String> approver2IdCaptor;
    
    
    private final byte[] content = "content".getBytes();
    
    private final String id = "id";
    
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
        ComplexIdentifiableContentApprover complexApprover = new ComplexIdentifiableContentApprover(
                approver1, approver2);
        doReturn(true).when(approver1).approve(id, content);
        doReturn(true).when(approver2).approve(id, content);
        
        // execute
        boolean result = complexApprover.approve(id, content);
        
        // assert
        assertTrue(result);
        verify(approver1).approve(approver1IdCaptor.capture(),
                approver1ContentCaptor.capture());
        verify(approver2).approve(approver2IdCaptor.capture(),
                approver2ContentCaptor.capture());
        assertTrue(content == approver1ContentCaptor.getValue());
        assertTrue(content == approver2ContentCaptor.getValue());
        assertTrue(id == approver1IdCaptor.getValue());
        assertTrue(id == approver2IdCaptor.getValue());
    }
    
    @Test
    public void testDisapproveFirst() throws Exception {
     // given
        ComplexIdentifiableContentApprover complexApprover = new ComplexIdentifiableContentApprover(
                approver1, approver2);
        doReturn(false).when(approver1).approve(id, content);
        
        // execute
        boolean result = complexApprover.approve(id, content);
        
        // assert
        assertFalse(result);
        verify(approver1, times(1)).approve(approver1IdCaptor.capture(),
                approver1ContentCaptor.capture());
        verify(approver2, never()).approve(any(), any());
        assertTrue(content == approver1ContentCaptor.getValue());
        assertTrue(id == approver1IdCaptor.getValue());
    }

    @Test
    public void testDisapproveLast() throws Exception {
        // given
        ComplexIdentifiableContentApprover complexApprover = new ComplexIdentifiableContentApprover(
                approver1, approver2);
        doReturn(true).when(approver1).approve(id, content);
        doReturn(false).when(approver2).approve(id, content);
        
        // execute
        boolean result = complexApprover.approve(id, content);
        
        // assert
        assertFalse(result);
        verify(approver1).approve(approver1IdCaptor.capture(),
                approver1ContentCaptor.capture());
        verify(approver2).approve(approver2IdCaptor.capture(),
                approver2ContentCaptor.capture());
        assertTrue(content == approver1ContentCaptor.getValue());
        assertTrue(content == approver2ContentCaptor.getValue());
        assertTrue(id == approver1IdCaptor.getValue());
        assertTrue(id == approver2IdCaptor.getValue());
    }
    
}

