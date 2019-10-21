package eu.dnetlib.iis.wf.export.actionmanager.entity;

import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

public class ConfidenceLevelUtils {

    private ConfidenceLevelUtils() {
    }

    public static Float evaluateConfidenceLevelThreshold(String trustLevelThreshold) {
        if (StringUtils.isNotBlank(trustLevelThreshold) &&
                !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(trustLevelThreshold)) {
            return Float.parseFloat(trustLevelThreshold) / InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR;
        }
        return null;
    }

    public static Boolean isValidConfidenceLevel(Float confidenceLevel, Float confidenceLevelThreshold) {
        return Objects.isNull(confidenceLevelThreshold) || confidenceLevel >= confidenceLevelThreshold;
    }

}
