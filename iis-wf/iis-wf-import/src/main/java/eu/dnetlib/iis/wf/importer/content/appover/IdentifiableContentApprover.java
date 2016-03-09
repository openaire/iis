package eu.dnetlib.iis.wf.importer.content.appover;


/**
 * Identifiable content approver interface.
 * @author mhorst
 *
 */
public interface IdentifiableContentApprover {

	/**
	 * Approves content provided as parameter.
	 * @param id content identifier
	 * @param content
	 * @return true when content approved, false when rejected
	 */
	boolean approve(String id, byte[] content);
}
