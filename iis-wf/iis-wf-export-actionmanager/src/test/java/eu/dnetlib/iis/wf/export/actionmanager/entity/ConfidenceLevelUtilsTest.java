package eu.dnetlib.iis.wf.export.actionmanager.entity;

import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import org.junit.Test;

import static org.junit.Assert.*;

public class ConfidenceLevelUtilsTest {

    @Test
    public void evaluateConfidenceLevelThresholdShouldReturnNullWhenThresholdIsNull() {
        //then
        assertNull(ConfidenceLevelUtils.evaluateConfidenceLevelThreshold(null));
    }

    @Test
    public void evaluateConfidenceLevelThresholdShouldReturnNullWhenThresholdIsEmpty() {
        //then
        assertNull(ConfidenceLevelUtils.evaluateConfidenceLevelThreshold(""));
    }

    @Test
    public void evaluateConfidenceLevelThresholdShouldReturnNullWhenThresholdIsWhitespace() {
        //then
        assertNull(ConfidenceLevelUtils.evaluateConfidenceLevelThreshold(" "));
    }

    @Test
    public void evaluateConfidenceLevelThresholdShouldReturnNullWhenThresholdEqualsUndefinedValue() {
        //then
        assertNull(ConfidenceLevelUtils.evaluateConfidenceLevelThreshold(WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE));
    }

    @Test
    public void evaluateConfidenceLevelThresholdShouldReturnProperForValidTHreshold() {
        //given
        String trustLevelThreshold = "0.7";

        //then
        Float actualThreshold = ConfidenceLevelUtils.evaluateConfidenceLevelThreshold(trustLevelThreshold);

        //then
        Float expectedThreshold = Float.parseFloat(trustLevelThreshold) / InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR;
        assertEquals(expectedThreshold, actualThreshold);
    }

    @Test
    public void isValidConfidenceLevelShouldReturnTrueWhenThresholdIsNull() {
        //then
        assertTrue(ConfidenceLevelUtils.isValidConfidenceLevel(1.0f, null));
    }

    @Test
    public void isValidConfidenceLevelShouldReturnTrueWhenConfidenceLevelIsAboveThreshold() {
        //then
        assertTrue(ConfidenceLevelUtils.isValidConfidenceLevel(1.0f, 0.9f));
    }

    @Test
    public void isValidConfidenceLevelShouldReturnTrueWhenConfidenceLevelIsEqualToThreshold() {
        //then
        assertTrue(ConfidenceLevelUtils.isValidConfidenceLevel(1.0f, 1.0f));
    }

    @Test
    public void isValidConfidenceLevelShouldReturnFalseWhenConfidenceLevelIsBelowThreshold() {
        //then
        assertFalse(ConfidenceLevelUtils.isValidConfidenceLevel(0.9f, 1.0f));
    }
}