package eu.dnetlib.iis.wf.export.actionmanager.entity;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.model.conversion.ConfidenceAndTrustLevelConversionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * Common confidence level related utility class.
 */
public class ConfidenceLevelUtils {

    private ConfidenceLevelUtils() {
    }

    /**
     * Evaluates confidence level threshold.
     *
     * @param trustLevelThreshold String corresponding to trust level threshold.
     * @return Float representing trust level threshold.
     */
    public static Float evaluateConfidenceLevelThreshold(String trustLevelThreshold) {
        if (StringUtils.isNotBlank(trustLevelThreshold) &&
                !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(trustLevelThreshold)) {
            return ConfidenceAndTrustLevelConversionUtils.trustLevelToConfidenceLevel(Float.parseFloat(trustLevelThreshold));
        }
        return null;
    }

    /**
     * Validates given confidence level against threshold.
     *
     * @param confidenceLevel          Confidence level to validate.
     * @param confidenceLevelThreshold Confidence level threshold.
     * @return True if given confidence level is valid, false otherwise.
     */
    public static Boolean isValidConfidenceLevel(Float confidenceLevel, Float confidenceLevelThreshold) {
        return Objects.isNull(confidenceLevelThreshold) || confidenceLevel >= confidenceLevelThreshold;
    }

}
