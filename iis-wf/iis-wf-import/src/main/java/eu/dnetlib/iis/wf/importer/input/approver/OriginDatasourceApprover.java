package eu.dnetlib.iis.wf.importer.input.approver;

import java.util.Collection;

import eu.dnetlib.data.proto.FieldTypeProtos.KeyValue;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.TypeProtos;

/**
 * Approver module verifying datasource current record originates from.
 * @author mhorst
 *
 * @param <T>
 */
public class OriginDatasourceApprover implements ResultApprover {

	
	/**
	 * Datasource identifiers which should be approved.
	 */
	private final Collection<String> datasourceIds;

	/**
	 * Entity types to be verified.
	 */
	private final Collection<TypeProtos.Type> entityTypes;
	
	/**
	 * Default constructor.
	 * @param entityTypes
	 * @param datasourceIds
	 */
	public OriginDatasourceApprover(
			Collection<TypeProtos.Type> entityTypes, 
			Collection<String> datasourceIds) {
		this.entityTypes = entityTypes;
		this.datasourceIds = datasourceIds;
	}
	
	/**
	 * Default constructor.
	 * @param datasourceIds
	 */
	public OriginDatasourceApprover(
			Collection<String> datasourceIds) {
		this(null, datasourceIds);
	}
	
	@Override
	public boolean approveBeforeBuilding(Oaf oaf) {
		if (oaf!=null && oaf.getEntity()!=null) {
//			checking whether this entity should be verified
			if (this.entityTypes!=null && !this.entityTypes.isEmpty()) {
				if (!this.entityTypes.contains(oaf.getEntity().getType())) {
//					this entity type should not be verified
					return true;
				}
			}
			if (oaf.getEntity().getCollectedfromCount()>0) {
				for (KeyValue currentCollectedFrom : oaf.getEntity().getCollectedfromList()) {
					if (datasourceIds==null || 
							datasourceIds.contains(currentCollectedFrom.getKey())) {
						return true;
					}
				}
			}
//			fallback
			return false;
		} else {
//			not an entity
			return true;
		}
	}

}
