package eu.dnetlib.iis.workflows.importer.input.approver;

import eu.dnetlib.data.proto.OafProtos.Oaf;

/**
 * Result approver interface.
 * Verifies whether provided object should be approved.
 * @author mhorst
 *
 */
public interface ResultApprover {

	
	/**
	 * Approves Oaf object, returns true when approved, false otherwise.
	 * This check is invoked before 
	 * AbstractHBaseIterator#buildObject() method execution.
	 * @param oaf
	 * @return true when approved, false otherwise
	 */
	boolean approveBeforeBuilding(Oaf oaf);

	
}
