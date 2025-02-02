package eu.dnetlib.iis.wf.export.actionmanager.module;

import org.apache.avro.specific.SpecificRecord;

import com.google.common.base.Preconditions;

import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.iis.common.model.conversion.ConfidenceAndTrustLevelConversionUtils;

/**
 * Abstract builder module.
 * 
 * @author mhorst
 *
 */
public abstract class AbstractBuilderModule<S extends SpecificRecord, T extends Oaf> implements ActionBuilderModule<S, T>{


    /**
     * Trust level threshold, null when not specified.
     */
    private final Float trustLevelThreshold;


    /**
     * Inference provenance.
     */
    private final String inferenceProvenance;

    

    // ------------------------ CONSTRUCTORS --------------------------

    public AbstractBuilderModule(Float trustLevelThreshold, String inferenceProvenance) {
        this.trustLevelThreshold = trustLevelThreshold;
        this.inferenceProvenance = Preconditions.checkNotNull(inferenceProvenance);
    }

    // ------------------------ GETTERS -------------------------------


    public String getInferenceProvenance() {
        return inferenceProvenance;
    }
    
    public Float getTrustLevelThreshold() {
        return trustLevelThreshold;
    }

    
    // ------------------------ LOGIC ---------------------------------


    /**
     * Returns {@link DataInfo} with inference details. Confidence level will be normalized to trust level.
     * 
     * @param confidenceLevel confidence level which will be normalized to trust level
     * @throws TrustLevelThresholdExceededException thrown when trust level threshold was exceeded
     */
    protected DataInfo buildInference(float confidenceLevel) throws TrustLevelThresholdExceededException {
        float currentTrustLevel = ConfidenceAndTrustLevelConversionUtils.confidenceLevelToTrustLevel(confidenceLevel);
        if (trustLevelThreshold == null || currentTrustLevel >= trustLevelThreshold) {
            return buildInferenceForTrustLevel(BuilderModuleHelper.getDecimalFormat().format(currentTrustLevel));    
        } else {
            throw new TrustLevelThresholdExceededException();
        }
    }
    
    /**
     * Returns {@link DataInfo} with inference details.
     * 
     */
    protected DataInfo buildInferenceForTrustLevel(String trustLevel) {
        return BuilderModuleHelper.buildInferenceForTrustLevel(trustLevel, inferenceProvenance);
    }
    
}
