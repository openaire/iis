package eu.dnetlib.iis.wf.importer.content.appover;

/**
 * Basic content approver checking PDF file header.
 * @author mhorst
 *
 */
public class PDFHeaderBasedContentApprover implements ContentApprover, IdentifiableContentApprover {

	@Override
	public boolean approve(byte[] data) {
		if (data == null || !(data.length > 4)) {
			return false;
		}
//		checking header
		if (data[0]==0x25 && 			// %
				data[1]==0x50 &&		// P
				data[2]==0x44 &&		// D
				data[3]==0x46 &&		// F
				data[4]==0x2D) {		// -
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean approve(String id, byte[] content) {
		return approve(content);
	}

}
