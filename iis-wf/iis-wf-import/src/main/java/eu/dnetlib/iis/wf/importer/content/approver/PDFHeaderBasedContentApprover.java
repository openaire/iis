package eu.dnetlib.iis.wf.importer.content.approver;

/**
 * Basic content approver checking PDF file header.
 * @author mhorst
 *
 */
public class PDFHeaderBasedContentApprover implements ContentApprover, IdentifiableContentApprover {

    // ------------------------ LOGIC --------------------------
    
	@Override
	public boolean approve(byte[] data) {
		if (data == null || data.length < 5) {
			return false;
		}
		return isValidPdfHeader(data);
	}

	@Override
	public boolean approve(String id, byte[] content) {
		return approve(content);
	}

	// ------------------------ PRIVATE --------------------------
	
	private boolean isValidPdfHeader(byte[] data) {
	    return data[0]==0x25 &&            // %
                data[1]==0x50 &&        // P
                data[2]==0x44 &&        // D
                data[3]==0x46 &&        // F
                data[4]==0x2D;         // -
	}
	
}
