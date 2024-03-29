package eu.dnetlib.iis.wf.importer.content.approver;

/**
 * Content approver interface.
 * @author mhorst
 *
 */
public interface ContentApprover {

	/**
	 * Approves content provided as parameter.
	 * @param content
	 * @return true when content approved, false when rejected
	 */
	boolean approve(byte[] content);
	
}
