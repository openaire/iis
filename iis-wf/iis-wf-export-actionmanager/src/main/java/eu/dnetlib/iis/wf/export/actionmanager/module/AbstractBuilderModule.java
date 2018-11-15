package eu.dnetlib.iis.wf.export.actionmanager.module;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.actionmanager.actions.ActionFactory;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.data.proto.FieldTypeProtos.DataInfo;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.iis.common.InfoSpaceConstants;

/**
 * Abstract builder module.
 * 
 * @author mhorst
 *
 */
public abstract class AbstractBuilderModule<T> implements ActionBuilderModule<T>{

    /**
     * Trust level threshold, null when not specified.
     */
    private final Float trustLevelThreshold;

    /**
     * Inference provenance.
     */
    private final String inferenceProvenance;

    /**
     * Action manager agent.
     */
    private final Agent agent;
    
    /**
     * Action set identifier.
     */
    private final String actionSetId;
    
    /**
     * Action factory to be used for building actions.
     */
    private final ActionFactory actionFactory = new ActionFactory();

    

    // ------------------------ CONSTRUCTORS --------------------------

    public AbstractBuilderModule(Float trustLevelThreshold, String inferenceProvenance,
            Agent agent, String actionSetId) {
        this.trustLevelThreshold = trustLevelThreshold;
        this.inferenceProvenance = Preconditions.checkNotNull(inferenceProvenance);
        this.agent = Preconditions.checkNotNull(agent);
        this.actionSetId = Preconditions.checkNotNull(actionSetId);
    }

    // ------------------------ GETTERS -------------------------------


    public String getInferenceProvenance() {
        return inferenceProvenance;
    }
    
    public Agent getAgent() {
        return agent;
    }
    
    public String getActionSetId() {
        return actionSetId;
    }

    public ActionFactory getActionFactory() {
        return actionFactory;
    }
    
    
    // ------------------------ LOGIC ---------------------------------

    /**
     * Builds {@link Oaf} object for given entity.
     * 
     */
    protected Oaf buildOaf(OafEntity oafEntity) {
        return BuilderModuleHelper.buildOaf(oafEntity, null);
    }

    /**
     * Returns {@link DataInfo} with inference details. Confidence level will be normalized to trust level.
     * 
     * @param confidenceLevel confidence level which will be normalized to trust level
     * @throws TrustLevelThresholdExceededException thrown when trust level threshold was exceeded
     */
    protected DataInfo buildInference(float confidenceLevel) throws TrustLevelThresholdExceededException {
        return buildInference(confidenceLevel, null);
    }
    
    /**
     * Returns {@link DataInfo} with inference details. Confidence level will be normalized to trust level.
     * 
     * @param confidenceLevel confidence level which will be normalized to trust level
     * @throws TrustLevelThresholdExceededException thrown when trust level threshold was exceeded
     */
    protected DataInfo buildInference(float confidenceLevel, String inferenceProvenance) throws TrustLevelThresholdExceededException {
        float currentTrustLevel = confidenceLevel * InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR;
        if (trustLevelThreshold == null || currentTrustLevel >= trustLevelThreshold) {
            if (StringUtils.isNotBlank(inferenceProvenance)) {
                return buildInferenceForTrustLevel(BuilderModuleHelper.getDecimalFormat().format(currentTrustLevel), 
                        inferenceProvenance);
            } else {
                return buildInferenceForTrustLevel(BuilderModuleHelper.getDecimalFormat().format(currentTrustLevel));    
            }
        } else {
            throw new TrustLevelThresholdExceededException();
        }
    }
    
    /**
     * Returns {@link DataInfo} with inference details.
     * 
     */
    protected DataInfo buildInferenceForTrustLevel(String trustLevel) {
        return buildInferenceForTrustLevel(trustLevel, inferenceProvenance);
    }

    /**
     * Returns {@link DataInfo} with inference details.
     * 
     */
    protected DataInfo buildInferenceForTrustLevel(String trustLevel, String inferenceProvenance) {
        return BuilderModuleHelper.buildInferenceForTrustLevel(trustLevel, inferenceProvenance);
    }
    
}
