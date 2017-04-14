package eu.dnetlib.iis.wf.importer.content.approver;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * @author mhorst
 *
 */
public class DeduplicationIdentifiableContentApproverTest {

    private final byte[] content = "content".getBytes();
    
    private final String id = "id";
    
    DeduplicationIdentifiableContentApprover approver = new DeduplicationIdentifiableContentApprover();

    // ------------------------------------ TESTS ------------------------------------
    
    @Test
    public void testApproveOnTheSameId() throws Exception {
        assertTrue(approver.approve(id, content));
        assertFalse(approver.approve(id, content));
    }
    
    @Test
    public void testApproveOnDifferentIds() throws Exception {
        assertTrue(approver.approve(id, content));
        assertTrue(approver.approve("differentId", content));
    }
    
}
