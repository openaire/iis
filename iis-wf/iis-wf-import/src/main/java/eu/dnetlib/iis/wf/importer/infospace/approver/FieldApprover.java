package eu.dnetlib.iis.wf.importer.infospace.approver;

import eu.dnetlib.dhp.schema.oaf.DataInfo;

/**
 * Verifies whether field with provided {@link DataInfo} should be approved.
 * 
 * @author mhorst
 *
 */
public interface FieldApprover {

	/**
	 * Approves given {@link DataInfo} object. returns true when approved, false otherwise.
	 */
	boolean approve(DataInfo dataInfo);
}
