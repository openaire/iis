package eu.dnetlib.iis.wf.importer.input.approver;

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

import eu.dnetlib.data.proto.KindProtos.Kind;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.iis.wf.importer.infospace.approver.ComplexApprover;
import eu.dnetlib.iis.wf.importer.infospace.approver.ResultApprover;

/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
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
    public void testApprove() throws Exception {
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
        assertTrue(oaf == approver1Captor.getValue());
        assertTrue(oaf == approver2Captor.getValue());
        
    }
    
    
    @Test
    public void testDisapproveFirst() throws Exception {
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
        assertTrue(oaf == approver1Captor.getValue());
    }

    @Test
    public void testDisapproveLast() throws Exception {
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
        assertTrue(oaf == approver1Captor.getValue());
        assertTrue(oaf == approver2Captor.getValue());
    }
    
    // ----------------------------- PRIVATE --------------------------

    private Oaf buildOaf() {
        return Oaf.newBuilder().setKind(Kind.entity).build();
    }
    
}
