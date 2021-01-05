package eu.dnetlib.iis.common.model.conversion;

import eu.dnetlib.iis.common.InfoSpaceConstants;

import java.util.Objects;

/**
 * Allows to convert confidence level to trust level and backwards.
 */
public class ConfidenceAndTrustLevelConversionUtils {
    private static final float confidenceToTrustConversionFactor = InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR;
    private static final float trustToConfidenceConversionFactor = 1 / InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR;

    private ConfidenceAndTrustLevelConversionUtils() {
    }

    /**
     * Converts confidence level to trust level using confidence to trust level scaling factor based on
     * {@link InfoSpaceConstants#CONFIDENCE_TO_TRUST_LEVEL_FACTOR}. Provides null safety.
     *
     * @param confidenceLevel Confidence level to be converted.
     * @return Float with corresponding trust level value.
     */
    public static Float confidenceLevelToTrustLevel(Float confidenceLevel) {
        return Objects.nonNull(confidenceLevel) ? confidenceLevel * confidenceToTrustConversionFactor : null;
    }

    /**
     * Converts confidence level to trust level using confidence to trust level scaling factor based on
     * {@link InfoSpaceConstants#CONFIDENCE_TO_TRUST_LEVEL_FACTOR}. This method should be used to avoid NPE warnings
     * when using primitive type for resulting trust level value.
     *
     * @param confidenceLevel Confidence level to be converted.
     * @return float with corresponding trust level value.
     */
    public static float confidenceLevelToTrustLevel(float confidenceLevel) {
        return confidenceLevel * confidenceToTrustConversionFactor;
    }

    /**
     * Converts trust level to confidence level using trust to confidence level scaling factor based on
     * {@link InfoSpaceConstants#CONFIDENCE_TO_TRUST_LEVEL_FACTOR}. Provides null safety.
     *
     * @param trustLevel Trust level to be converted.
     * @return Float corresponding to confidence level value.
     */
    public static Float trustLevelToConfidenceLevel(Float trustLevel) {
        return Objects.nonNull(trustLevel) ? trustLevel * trustToConfidenceConversionFactor : null;
    }

    /**
     * Converts trust level to confidence level using trust to confidence level scaling factor based on
     * {@link InfoSpaceConstants#CONFIDENCE_TO_TRUST_LEVEL_FACTOR}. This method should be used to avoid NPE warnings
     * when using primitive type for resulting confidence level value.
     *
     * @param trustLevel Trust level to be converted.
     * @return float corresponding to confidence level value.
     */
    public static float trustLevelToConfidenceLevel(float trustLevel) {
        return trustLevel * trustToConfidenceConversionFactor;
    }

}
