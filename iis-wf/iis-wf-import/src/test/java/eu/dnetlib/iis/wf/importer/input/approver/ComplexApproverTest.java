package eu.dnetlib.iis.wf.importer.input.approver;

import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.iis.wf.importer.infospace.approver.ComplexApprover;
import eu.dnetlib.iis.wf.importer.infospace.approver.ResultApprover;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author mhorst
 *
 */
@ExtendWith(MockitoExtension.class)
public class ComplexApproverTest {

    @Mock
    private ResultApprover approver1;
    
    @Mock
    private ResultApprover approver2;
    
    @Captor
    private ArgumentCaptor<Oaf> approver1Captor;
    
    @Captor
    private ArgumentCaptor<Oaf> approver2Captor;
    
    
    // ----------------------------- TESTS ----------------------------
    
    @Test
    public void testApproveEmpty() throws Exception {
        // given
        Oaf oaf = buildOaf();
        ComplexApprover complexApprover = new ComplexApprover();
        
        // execute & assert
        assertTrue(complexApprover.approve(oaf));
    }

    @Test
    public void testApprove() {
        // given
        Oaf oaf = buildOaf();
        ComplexApprover complexApprover = new ComplexApprover(approver1, approver2);
        doReturn(true).when(approver1).approve(oaf);
        doReturn(true).when(approver2).approve(oaf);
        
        
        // execute
        boolean result = complexApprover.approve(oaf);
        
        // assert
        assertTrue(result);
        verify(approver1).approve(approver1Captor.capture());
        verify(approver2).approve(approver2Captor.capture());
        assertSame(oaf, approver1Captor.getValue());
        assertSame(oaf, approver2Captor.getValue());
        
    }
    
    
    @Test
    public void testDisapproveFirst() {
     // given
        Oaf oaf = buildOaf();
        ComplexApprover complexApprover = new ComplexApprover(approver1, approver2);
        doReturn(false).when(approver1).approve(oaf);
        
        // execute
        boolean result = complexApprover.approve(oaf);
        
        // assert
        assertFalse(result);
        verify(approver1, times(1)).approve(approver1Captor.capture());
        verify(approver2, never()).approve(approver2Captor.capture());
        assertSame(oaf, approver1Captor.getValue());
    }

    @Test
    public void testDisapproveLast() {
     // given
        Oaf oaf = buildOaf();
        ComplexApprover complexApprover = new ComplexApprover(approver1, approver2);
        doReturn(true).when(approver1).approve(oaf);
        doReturn(false).when(approver2).approve(oaf);
        
        
        // execute
        boolean result = complexApprover.approve(oaf);
        
        // assert
        assertFalse(result);
        verify(approver1).approve(approver1Captor.capture());
        verify(approver2).approve(approver2Captor.capture());
        assertSame(oaf, approver1Captor.getValue());
        assertSame(oaf, approver2Captor.getValue());
    }
    
    // ----------------------------- PRIVATE --------------------------

    private Oaf buildOaf() {
        return new Publication();
    }
    
}
