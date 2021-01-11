package eu.dnetlib.iis.common.model.conversion;

import eu.dnetlib.iis.common.InfoSpaceConstants;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ConfidenceAndTrustLevelConversionUtilsTest {

    @Nested
    public class ConfidenceLevelToTrustLevelConversionTest {

        @Test
        @DisplayName("Null confidence level is converted to null trust level")
        public void givenNullConfidenceLevel_whenConvertedToTrustLevel_thenNullIsReturned() {
            Float result = ConfidenceAndTrustLevelConversionUtils.confidenceLevelToTrustLevel(null);

            assertNull(result);
        }

        @Test
        @DisplayName("Confidence level float object is converted to trust level float object using scaling factor")
        public void givenConfidenceLevelAsFloatObject_whenConvertedToTrustLevel_thenProperValueIsReturned() {
            Float result = ConfidenceAndTrustLevelConversionUtils.confidenceLevelToTrustLevel(Float.valueOf(0.5f));

            assertEquals(0.5f * InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR, result);
        }

        @Test
        @DisplayName("Confidence level float is converted to trust level float using scaling factor")
        public void givenConfidenceLevelAsFloat_whenConvertedToTrustLevel_thenProperValueIsReturned() {
            float result = ConfidenceAndTrustLevelConversionUtils.confidenceLevelToTrustLevel(0.5f);

            assertEquals(0.5f * InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR, result);
        }
    }

    @Nested
    public class TrustLevelToConfidenceLevelConversionTest {

        @Test
        @DisplayName("Null trust level is converted to null confidence level")
        public void givenNullTrustLevel_whenConvertedToConfidenceLevel_thenNullIsReturned() {
            Float result = ConfidenceAndTrustLevelConversionUtils.trustLevelToConfidenceLevel(null);

            assertNull(result);
        }

        @Test
        @DisplayName("Trust level float object is converted to confidence level float object using scaling factor")
        public void givenTrustLevelAsFloatObject_whenConvertedToConfidenceLevel_thenProperValueIsReturned() {
            Float result = ConfidenceAndTrustLevelConversionUtils.trustLevelToConfidenceLevel(Float.valueOf(0.5f));

            assertEquals(0.5f / InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR, result);
        }

        @Test
        @DisplayName("Trust level float is converted to confidence level float using scaling factor")
        public void givenTrustLevelAsFloat_whenConvertedToConfidenceLevel_thenProperValueIsReturned() {
            float result = ConfidenceAndTrustLevelConversionUtils.trustLevelToConfidenceLevel(0.5f);

            assertEquals(0.5f / InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR, result);
        }
    }
}