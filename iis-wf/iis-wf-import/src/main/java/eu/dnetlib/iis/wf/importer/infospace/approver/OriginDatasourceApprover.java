package eu.dnetlib.iis.wf.importer.infospace.approver;

import java.util.Collection;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.data.proto.FieldTypeProtos.KeyValue;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.TypeProtos;

/**
 * Approver module verifying datasource current record originates from.
 * 
 * @author mhorst
 *
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

    // ------------------------ CONSTRUCTORS --------------------------
    
    /**
     * @param entityTypes entity types to be checked, other types are approved
     * @param datasourceIds set of datasource identifiers to be approved
     */
    public OriginDatasourceApprover(Collection<TypeProtos.Type> entityTypes, Collection<String> datasourceIds) {
        Preconditions.checkNotNull(this.datasourceIds = datasourceIds);
        this.entityTypes = entityTypes;
    }

    /**
     * @param datasourceIds set of datasource identifiers to be approved
     */
    public OriginDatasourceApprover(Collection<String> datasourceIds) {
        this(null, datasourceIds);
    }

    // ------------------------ LOGIC --------------------------
    
    @Override
    public boolean approve(Oaf oaf) {
        if (oaf == null || oaf.getEntity() == null) {
            // not an entity
            return true;
        }
        // checking whether this entity should be verified
        if (!CollectionUtils.isEmpty(this.entityTypes) && 
                !this.entityTypes.contains(oaf.getEntity().getType())) {
            // this entity type should not be verified
            return true;
        }
        // checking whether datasource is approved
        if (!CollectionUtils.isEmpty(oaf.getEntity().getCollectedfromList())) {
            for (KeyValue currentCollectedFrom : oaf.getEntity().getCollectedfromList()) {
                if (datasourceIds.contains(currentCollectedFrom.getKey())) {
                    return true;
                }
            }
        }
        // fallback
        return false;
    }

}
