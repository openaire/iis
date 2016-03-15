package eu.dnetlib.iis.wf.importer.input.approver;

import eu.dnetlib.data.proto.FieldTypeProtos.DataInfo;

/**
 * Field approver interface.
 * Verifies whether field with provided {@link DataInfo} should be approved.
 * @author mhorst
 *
 */
public interface FieldApprover {

	/**
	 * Approves given {@link DataInfo} object. returns true when approved, false otherwise.
	 * @param dataInfo
	 * @return true when approved, false otherwise
	 */
	boolean approve(DataInfo dataInfo);
}
