package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.iis.common.InfoSpaceConstants;

/**
 * {@link Oaf} builder helper.
 * 
 * @author mhorst
 *
 */
public class BuilderModuleHelper {

    /**
     * Trust level format.
     */
    private static final DecimalFormat decimalFormat = initailizeDecimalFormat();
    
    
    /**
     * Returns {@link DataInfo} with inference details including inferred flag set to true.
     * 
     */
    public static DataInfo buildInferenceForConfidenceLevel(
            float confidenceLevel, String inferenceProvenance) {
        return buildInferenceForTrustLevel(
                decimalFormat.format(confidenceLevel * InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR),
                inferenceProvenance);
    }
    
    /**
     * Returns {@link DataInfo} with inference details including inferred flag set to true.
     * 
     */
    public static DataInfo buildInferenceForTrustLevel(
            String trustLevel, String inferenceProvenance) {
        return buildInferenceForTrustLevel(true, trustLevel, inferenceProvenance, InfoSpaceConstants.SEMANTIC_CLASS_IIS);
    }

    /**
     * Returns {@link DataInfo} with inference details.
     * 
     */
    public static DataInfo buildInferenceForTrustLevel(boolean inferred, 
            String trustLevel, String inferenceProvenance, String provenanceClass) {
        DataInfo dataInfo = new DataInfo();
        dataInfo.setInferred(inferred);
        dataInfo.setTrust(trustLevel);
        Qualifier provenanceQualifier = new Qualifier();
        provenanceQualifier.setClassid(provenanceClass);
        provenanceQualifier.setClassname(provenanceClass);
        provenanceQualifier.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS);
        provenanceQualifier.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS);
        dataInfo.setProvenanceaction(provenanceQualifier);
        dataInfo.setInferenceprovenance(inferenceProvenance);
        return dataInfo;
    }
    
    /**
     * Provides predefined decimal format to be used for representing float values as String. 
     */
    public static DecimalFormat getDecimalFormat() {
        return decimalFormat;
    }
    
    // ----------------------------------- PRIVATE ----------------------------------
    
    private static DecimalFormat initailizeDecimalFormat() {
        DecimalFormat decimalFormat = (DecimalFormat) NumberFormat.getInstance(Locale.ENGLISH);
        decimalFormat.applyPattern("#.####");
        return decimalFormat;
    }
    
}
