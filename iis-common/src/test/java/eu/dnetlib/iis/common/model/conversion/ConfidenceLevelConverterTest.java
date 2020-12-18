package eu.dnetlib.iis.common.model.conversion;

import eu.dnetlib.iis.common.InfoSpaceConstants;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ConfidenceLevelConverterTest {

    private ConfidenceLevelConverter confidenceLevelConverter = new ConfidenceLevelConverter();

    @Test
    @DisplayName("Null confidence level is converted to null")
    public void givenConverter_whenNullValueIsConverted_thenNullIsReturned() {
        Float result = confidenceLevelConverter.convertToTrustLevel(null);

        assertNull(result);
    }

    @Test
    @DisplayName("Confidence level is converted to trust level using scaling factor")
    public void givenConverter_whenAFloatValueIsConverted_thenProperValueIsReturned() {
        float confidenceLevel = 0.5f;

        Float result = confidenceLevelConverter.convertToTrustLevel(confidenceLevel);

        assertEquals(confidenceLevel * InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR, result);
    }
}