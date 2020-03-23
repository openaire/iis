package eu.dnetlib.iis.wf.importer.infospace.approver;

import java.util.regex.Pattern;

import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Oaf;

/**
 * Inference data based result approver.
 * 
 * @author mhorst
 *
 */
public class DataInfoBasedApprover implements ResultApprover, FieldApprover {


    private static final long serialVersionUID = -1093513836478197899L;

    /**
     * List of blacklisted inference provenance values.
     * 
     */
    private final String inferenceProvenanceBlacklistPattern;

    /**
     * Flag indicating deleted by inference objects should be skipped.
     * 
     */
    private final boolean skipDeletedByInference;

    /**
     * Trust level threshold.
     * 
     */
    private final Float trustLevelThreshold;

    // ------------------------ CONSTRUCTORS --------------------------

    /**
     * @param inferenceProvenanceBlacklistPattern regex pattern matching inference provenance
     * @param skipDeletedByInference flag indicating records deleted by inference should be skipped
     * @param trustLevelThreshold trust level threshold, check is skipped when set to null
     */
    public DataInfoBasedApprover(String inferenceProvenanceBlacklistPattern, boolean skipDeletedByInference,
            Float trustLevelThreshold) {
        this.inferenceProvenanceBlacklistPattern = inferenceProvenanceBlacklistPattern;
        this.skipDeletedByInference = skipDeletedByInference;
        this.trustLevelThreshold = trustLevelThreshold;
    }

    // ------------------------ LOGIC --------------------------
    
    @Override
    public boolean approve(Oaf oaf) {
        if (oaf != null) {
            return approve(oaf.getDataInfo());
        } else {
            return false;
        }
    }

    /**
     * Makes decision based on inference data.
     * 
     */
    @Override
    public boolean approve(DataInfo dataInfo) {
        if (dataInfo != null) {
            if (inferenceProvenanceBlacklistPattern != null && dataInfo.getInferred() &&
                    Pattern.matches(inferenceProvenanceBlacklistPattern, dataInfo.getInferenceprovenance())) {
                return false;
            }
            if (skipDeletedByInference && dataInfo.getDeletedbyinference()) {
                return false;
            }
            if (dataInfo.getInvisible()) {
                return false;
            }
            if (trustLevelThreshold != null && dataInfo.getTrust() != null && !dataInfo.getTrust().isEmpty()
                    && Float.valueOf(dataInfo.getTrust()) < trustLevelThreshold) {
                return false;
            }
        }
        return true;
    }

}
