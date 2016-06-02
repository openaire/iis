package eu.dnetlib.iis.wf.export.actionmanager.module;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ALGORITHM_PROPERTY_SEPARATOR;
import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_TRUST_LEVEL_THRESHOLD;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.hbase.HBaseConstants;

/**
 * Abstract action builder factory.
 * 
 * @author mhorst
 *
 * @param <T> avro input type
 */
public abstract class AbstractBuilderFactory<T> implements ActionBuilderFactory<T> {

    /**
     * Algorithm name associated with builder.
     */
    protected final AlgorithmName algorithmName;

    // ------------------ CONSTRUCTORS -----------------------

    AbstractBuilderFactory(AlgorithmName algorithmName) {
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
        String algorithmTrustLevelThreshold = conf
                .get(EXPORT_TRUST_LEVEL_THRESHOLD + EXPORT_ALGORITHM_PROPERTY_SEPARATOR + algorithmName.name());
        if (algorithmTrustLevelThreshold != null
                && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(algorithmTrustLevelThreshold)) {
            return Float.valueOf(algorithmTrustLevelThreshold);
        }
        String defaultTrustLevelThresholdStr = conf.get(EXPORT_TRUST_LEVEL_THRESHOLD);
        if (defaultTrustLevelThresholdStr != null
                && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(defaultTrustLevelThresholdStr)) {
            return Float.valueOf(defaultTrustLevelThresholdStr);
        }
        // fallback: threshold was not defined
        return null;
    }

    /**
     * Builds inference provenance string based on algorithm name.
     */
    protected String buildInferenceProvenance() {
        return HBaseConstants.SEMANTIC_CLASS_IIS + HBaseConstants.INFERENCE_PROVENANCE_SEPARATOR
                + algorithmName.toString();
    }
    
}
