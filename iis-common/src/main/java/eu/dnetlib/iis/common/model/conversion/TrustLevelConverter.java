package eu.dnetlib.iis.common.model.conversion;

import eu.dnetlib.iis.common.InfoSpaceConstants;

import java.util.Objects;

/**
 * Allows to convert trust level to confidence level.
 */
public class TrustLevelConverter {
    private Float trustToConfidenceConversionFactor;

    public TrustLevelConverter() {
        this.trustToConfidenceConversionFactor = 1 / InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR;
    }

    /**
     * Converts trust level to confidence level using trust to confidence level scaling factor. Provides null safety.
     *
     * @param trustLevel Trust level to be converted.
     * @return Float corresponding to confidence level value.
     */
    public Float convertToConfidenceLevel(Float trustLevel) {
        return Objects.nonNull(trustLevel) ? trustLevel * trustToConfidenceConversionFactor : null;
    }
}
