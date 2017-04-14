package eu.dnetlib.iis.wf.importer.content.approver;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * @author mhorst
 *
 */
public class PDFHeaderBasedContentApproverTest {

    private final String id = "id";
    
    PDFHeaderBasedContentApprover approver = new PDFHeaderBasedContentApprover();

    // ------------------------------------ TESTS ------------------------------------
    
    @Test
    public void testApprove() throws Exception {
        assertTrue(approver.approve(id, "%PDF-1.5".getBytes("utf8")));
        assertTrue(approver.approve(id, "%PDF-".getBytes("utf8")));
    }
    
    @Test
    public void testDisapprove() throws Exception {
        assertFalse(approver.approve(id, "nonpdf".getBytes("utf8")));
        assertFalse(approver.approve(id, "%PDF".getBytes("utf8")));
        assertFalse(approver.approve(id, null));
    }
    
}

