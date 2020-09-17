package eu.dnetlib.iis.wf.importer.infospace.truncator;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StringTruncatorTest {

    @Test
    public void shouldNotTruncateStringShorterThanThreshold() {
        // when
        String result = StringTruncator.truncateWithoutWordSplit("abc def", 8);

        // then
        assertEquals("abc def", result);
    }

    @Test
    public void shouldNotTruncateStringEqualToThreshold() {
        // when
        String result = StringTruncator.truncateWithoutWordSplit("abc def", 7);

        // then
        assertEquals("abc def", result);
    }

    @Test
    public void shouldTruncateStringLongerThanThresholdAndKeepWordAtBoundary() {
        // when
        String result = StringTruncator.truncateWithoutWordSplit("abc def", 6);

        // then
        assertEquals("abc def", result);
    }

    @Test
    public void shouldTruncateStringLongerThanThreshold() {
        // when
        String result = StringTruncator.truncateWithoutWordSplit("abc def ghi", 6);

        // then
        assertEquals("abc def", result);
    }
}