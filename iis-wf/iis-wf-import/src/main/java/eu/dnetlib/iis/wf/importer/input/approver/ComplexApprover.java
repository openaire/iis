package eu.dnetlib.iis.wf.importer.input.approver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.dnetlib.data.proto.OafProtos.Oaf;

/**
 * Complex approver encapsulating multiple approvers.
 * @author mhorst
 *
 * @param <T>
 */
public class ComplexApprover implements ResultApprover {

	private final List<ResultApprover> approvers;
	
	/**
	 * Default constructor.
	 * @param approvers
	 */
	public ComplexApprover() {
		this.approvers = new ArrayList<ResultApprover>();
	}
	
	/**
	 * Parameterized constructor.
	 * @param approvers
	 */
	public ComplexApprover(ResultApprover ...approvers) {
		this.approvers = Arrays.asList(approvers);
	}
	
	
	@Override
	public boolean approveBeforeBuilding(Oaf oaf) {
		for (ResultApprover approver : approvers) {
			if (!approver.approveBeforeBuilding(oaf)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Appends approver.
	 * @param approver
	 */
	public void appendApprover(ResultApprover approver) {
		this.approvers.add(approver);
	}

}
