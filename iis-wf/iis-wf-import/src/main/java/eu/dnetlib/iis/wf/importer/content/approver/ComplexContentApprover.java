package eu.dnetlib.iis.wf.importer.content.approver;

import java.util.Arrays;
import java.util.List;

/**
 * Complex content approver holding multiple content approvers.
 * Evaluates to true whan all content approvers evalute to true.
 * @author mhorst
 *
 */
public class ComplexContentApprover implements ContentApprover {

	private final List<ContentApprover> approvers;
	
	public ComplexContentApprover(List<ContentApprover> approvers) {
		this.approvers = approvers;
	}
	
	public ComplexContentApprover(ContentApprover... approvers) {
		this.approvers = Arrays.asList(approvers);
	}
	
	@Override
	public boolean approve(byte[] content) {
		for (ContentApprover currentApprover : approvers) {
			if (!currentApprover.approve(content)) {
				return false;
			}
		}
		return true;
	}

}
