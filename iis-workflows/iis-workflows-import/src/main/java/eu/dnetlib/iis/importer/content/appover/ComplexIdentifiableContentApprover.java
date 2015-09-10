package eu.dnetlib.iis.importer.content.appover;

import java.util.Arrays;
import java.util.List;

/**
 * Complex content approver holding multiple content approvers.
 * Evaluates to true whan all content approvers evalute to true.
 * @author mhorst
 *
 */
public class ComplexIdentifiableContentApprover implements IdentifiableContentApprover {

	private final List<IdentifiableContentApprover> approvers;
	
	public ComplexIdentifiableContentApprover(List<IdentifiableContentApprover> approvers) {
		this.approvers = approvers;
	}

	public ComplexIdentifiableContentApprover(IdentifiableContentApprover... approvers) {
		this.approvers = Arrays.asList(approvers);
	}
	
	@Override
	public boolean approve(String id, byte[] content) {
		for (IdentifiableContentApprover currentApprover : approvers) {
			if (!currentApprover.approve(id, content)) {
				return false;
			}
		}
		return true;
	}

}
