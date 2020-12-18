package eu.dnetlib.iis.common.model.conversion;

import eu.dnetlib.iis.common.InfoSpaceConstants;

import java.util.Objects;

/**
 * Allows to convert confidence level to trust level.
 */
public class ConfidenceLevelConverter {
    private Float confidenceToTrustConversionFactor;

    public ConfidenceLevelConverter() {
        this.confidenceToTrustConversionFactor = InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR;
    }

    /**
     * Converts confidence level to trust level using confidence to trust level scaling factor. Provides null safety.
     *
     * @param confidenceLevel Confidence level to be converted.
     * @return Float with corresponding trust level value.
     */
    public Float convertToTrustLevel(Float confidenceLevel) {
        return Objects.nonNull(confidenceLevel) ? confidenceLevel * confidenceToTrustConversionFactor : null;
    }
}
