package eu.dnetlib.iis.wf.importer.infospace.approver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.dnetlib.data.proto.OafProtos.Oaf;

/**
 * Complex approver encapsulating multiple approvers.
 * 
 * @author mhorst
 * 
 */
public class ComplexApprover implements ResultApprover {

    private final List<ResultApprover> approvers;

    // ------------------------ CONSTRUCTORS --------------------------
    
    public ComplexApprover() {
        this.approvers = new ArrayList<ResultApprover>();
    }

    public ComplexApprover(ResultApprover... approvers) {
        this.approvers = Arrays.asList(approvers);
    }

    // ------------------------ LOGIC --------------------------
    
    @Override
    public boolean approve(Oaf oaf) {
        for (ResultApprover approver : approvers) {
            if (!approver.approve(oaf)) {
                return false;
            }
        }
        return true;
    }

}
