package eu.dnetlib.iis.wf.export.actionmanager.module;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ALGORITHM_PROPERTY_SEPARATOR;
import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_TRUST_LEVEL_THRESHOLD;

import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;

/**
 * Abstract action builder factory.
 * 
 * @author mhorst
 *
 * @param <S> avro input type
 * @param <T> target {@link Oaf} model object
 */
public abstract class AbstractActionBuilderFactory<S extends SpecificRecord, T extends Oaf> implements ActionBuilderFactory<S, T> {

    /**
     * Algorithm name associated with builder.
     */
    private final AlgorithmName algorithmName;

    // ------------------ CONSTRUCTORS -----------------------

    AbstractActionBuilderFactory(AlgorithmName algorithmName) {
        this.algorithmName = algorithmName;
    }

    // ------------------ LOGIC ------------------------------
    
    @Override
    public AlgorithmName getAlgorithName() {
        return algorithmName;
    }

    /**
     * Provides trust level threshold if defined for given algorithm or globally.
     * @param context
     * @return trust level threshold or null if not defined
     */
    protected Float provideTrustLevelThreshold(Configuration conf) {
        String trustLevelThresholdStr = WorkflowRuntimeParameters.getParamValue(
                EXPORT_TRUST_LEVEL_THRESHOLD + EXPORT_ALGORITHM_PROPERTY_SEPARATOR + algorithmName.name(), 
                EXPORT_TRUST_LEVEL_THRESHOLD, conf);
        if (trustLevelThresholdStr!=null) {
            return Float.valueOf(trustLevelThresholdStr);
        } else {
            return null;
        }
    }

    /**
     * Builds inference provenance string based on algorithm name.
     */
    protected String buildInferenceProvenance() {
        return InfoSpaceConstants.SEMANTIC_CLASS_IIS + InfoSpaceConstants.INFERENCE_PROVENANCE_SEPARATOR
                + algorithmName.toString();
    }
    
}
