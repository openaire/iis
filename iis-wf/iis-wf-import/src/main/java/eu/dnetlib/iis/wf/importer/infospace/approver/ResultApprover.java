package eu.dnetlib.iis.wf.importer.infospace.approver;

import eu.dnetlib.data.proto.OafProtos.Oaf;

/**
 * Verifies whether provided object should be approved.
 * 
 * @author mhorst
 *
 */
public interface ResultApprover {

	
	/**
	 * Approves Oaf object. Returns true when approved, false otherwise.
	 * @param oaf {@link Oaf} object to be approved
	 */
	boolean approve(Oaf oaf);
	
}
