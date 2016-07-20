package eu.dnetlib.iis.wf.importer.content.approver;

/**
 * Size limit based content approver.
 * @author mhorst
 *
 */
public class SizeLimitContentApprover implements ContentApprover, IdentifiableContentApprover {

	/**
	 * Predefined size limit.
	 */
	private final int sizeLimit;
	
	
	/**
	 * Default constructor.
	 * @param sizeLimitMegabytes size limit in megabytes
	 */
	public SizeLimitContentApprover(int sizeLimitMegabytes) {
		this.sizeLimit = sizeLimitMegabytes * 1024 * 1024;
	}
	
	@Override
	public boolean approve(String id, byte[] content) {
		return approve(content);
	}

	@Override
	public boolean approve(byte[] content) {
		return content==null || content.length<sizeLimit;
	}

}
