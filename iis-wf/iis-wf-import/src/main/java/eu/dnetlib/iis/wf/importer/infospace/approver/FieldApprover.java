package eu.dnetlib.iis.wf.importer.infospace.approver;

import java.io.Serializable;

import eu.dnetlib.dhp.schema.oaf.DataInfo;

/**
 * Verifies whether field with provided {@link DataInfo} should be approved.
 * 
 * @author mhorst
 *
 */
public interface FieldApprover extends Serializable {

	/**
	 * Approves given {@link DataInfo} object. returns true when approved, false otherwise.
	 */
	boolean approve(DataInfo dataInfo);
}
