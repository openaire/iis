package eu.dnetlib.iis.wf.importer.content.approver;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

