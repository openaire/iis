package eu.dnetlib.iis.common.model.conversion;

import eu.dnetlib.iis.common.InfoSpaceConstants;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TrustLevelConverterTest {

    private TrustLevelConverter trustLevelConverter = new TrustLevelConverter();

    @Test
    @DisplayName("Null trust level is converted to null")
    public void givenConverter_whenNullValueIsConverted_thenNullIsReturned() {
        Float result = trustLevelConverter.convertToConfidenceLevel(null);

        assertNull(result);
    }

    @Test
    @DisplayName("Trust level is converted to confidence level using scaling factor")
    public void givenConverter_whenAFloatValueIsConverted_thenProperValueIsReturned() {
        float trustLevel = 0.5f;

        Float result = trustLevelConverter.convertToConfidenceLevel(trustLevel);

        assertEquals(trustLevel / InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR, result);
    }
}