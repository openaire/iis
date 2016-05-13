package eu.dnetlib.iis.wf.importer.infospace.approver;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.ResultProtos.Result.Instance;

/**
 * Publication type based approver.
 * 
 * @author mhorst
 *
 */
public class PublicationTypeApprover implements ResultApprover {

    /**
     * Supported publication type.
     */
    private final String publicationType;

    // ------------------------ CONSTRUCTORS --------------------------
    
    /**
     * @param publicationType publication type to be accepted
     */
    public PublicationTypeApprover(String publicationType) {
        Preconditions.checkNotNull(this.publicationType = publicationType);
    }

    // ------------------------ LOGIC --------------------------        
    
    @Override
    public boolean approve(Oaf oaf) {
        if (oaf != null && oaf.getEntity() != null && oaf.getEntity().getResult() != null
                && !CollectionUtils.isEmpty(oaf.getEntity().getResult().getInstanceList())) {
            for (Instance currentInstance : oaf.getEntity().getResult().getInstanceList()) {
                if (currentInstance.getInstancetype() != null
                        && publicationType.equals(currentInstance.getInstancetype().getClassid())) {
                    return true;
                }
            }
        }
        // fallback
        return false;
    }

}
