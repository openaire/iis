package eu.dnetlib.iis.wf.importer.input.approver;

import java.util.regex.Pattern;

import eu.dnetlib.data.proto.FieldTypeProtos.DataInfo;
import eu.dnetlib.data.proto.OafProtos.Oaf;

/**
 * Inference data based result approver.
 * @author mhorst
 *
 */
public class DataInfoBasedApprover implements ResultApprover, FieldApprover {

	
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
	
	/**
	 * Default constructor.
	 * @param inferenceProvenanceBlacklistPattern
	 * @param skipDeletedByInference
	 * @param trustLevelThreshold threshold check is skipped when set to null
	 */
	public DataInfoBasedApprover(String inferenceProvenanceBlacklistPattern,
			boolean skipDeletedByInference, Float trustLevelThreshold) {
		this.inferenceProvenanceBlacklistPattern = inferenceProvenanceBlacklistPattern;
		this.skipDeletedByInference = skipDeletedByInference;
		this.trustLevelThreshold = trustLevelThreshold;
	}

	@Override
	public boolean approveBeforeBuilding(Oaf oaf) {
		if (oaf!=null) {
			return approve(oaf.getDataInfo());
		} else {
			return false;
		}
	}

	/**
	 * Checks whether object should be omitted.
	 * @param oaf
	 * @return true when should be omitted, false otherwise
	 */
	public boolean approve(DataInfo dataInfo) {
//		checking inference data
		if (dataInfo!=null) {
			if (inferenceProvenanceBlacklistPattern!=null && dataInfo.getInferred()) {
				if (Pattern.matches(inferenceProvenanceBlacklistPattern, dataInfo.getInferenceprovenance())) {
					return false;
				}
			}
			if (skipDeletedByInference && 
					dataInfo.getDeletedbyinference()) {
				return false;
			}
			if (trustLevelThreshold!=null && 
					dataInfo.getTrust()!=null && !dataInfo.getTrust().isEmpty()) {
				if (Float.valueOf(dataInfo.getTrust())<trustLevelThreshold) {
					return false;
				}
			}
		}
		return true;
	}
	
}
