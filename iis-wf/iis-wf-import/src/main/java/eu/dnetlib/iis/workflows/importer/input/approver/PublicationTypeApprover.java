package eu.dnetlib.iis.workflows.importer.input.approver;

import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.ResultProtos.Result.Instance;

/**
 * Publication type based approver.
 * @author mhorst
 *
 * @param <T>
 */
public class PublicationTypeApprover implements ResultApprover {

	/**
	 * Supported publication type.
	 */
	private final String publicationType;
	
	/**
	 * Default constructor.
	 * @param publicationType
	 */
	public PublicationTypeApprover(String publicationType) {
		this.publicationType = publicationType;
	}
	
	@Override
	public boolean approveBeforeBuilding(Oaf oaf) {
		if (oaf!=null && oaf.getEntity()!=null && oaf.getEntity().getResult()!=null && 
				oaf.getEntity().getResult().getInstanceCount()>0) {
			for (Instance currentInstance : oaf.getEntity().getResult().getInstanceList()) {
				if (currentInstance.getInstancetype()!=null &&
						publicationType.equals(
								currentInstance.getInstancetype().getClassid())) {
					return true;
				}
			}
		}
//		fallback
		return false;
	}

}
