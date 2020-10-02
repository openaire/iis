package eu.dnetlib.iis.common.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author mhorst
 *
 */
public class ByteArrayUtilsTest {

    private static final String ENCODING_UTF8 = "utf8";

    @Test
    public void testStartsWithNullSource() {
        assertThrows(NullPointerException.class, () ->
                ByteArrayUtils.startsWith(null, "str".getBytes(ENCODING_UTF8)));
    }
    
    @Test
    public void testStartsWithNullMatch() {
        assertThrows(NullPointerException.class, () ->
                ByteArrayUtils.startsWith("str".getBytes(ENCODING_UTF8), null));
    }
    
    @Test
    public void testStartsWith() throws Exception {
        // execute & assert
        assertFalse(ByteArrayUtils.startsWith(
                "strA".getBytes(ENCODING_UTF8), "strB".getBytes(ENCODING_UTF8)));
        assertTrue(ByteArrayUtils.startsWith(
                "strA".getBytes(ENCODING_UTF8), "str".getBytes(ENCODING_UTF8)));
    }

    @Test
    public void testStartsWithOffset() throws Exception {
        // execute & assert
        assertFalse(ByteArrayUtils.startsWith(
                "prefix value".getBytes(ENCODING_UTF8), 6, "value".getBytes(ENCODING_UTF8)));
        assertTrue(ByteArrayUtils.startsWith(
                "prefix value".getBytes(ENCODING_UTF8), 7, "value".getBytes(ENCODING_UTF8)));
        assertFalse(ByteArrayUtils.startsWith(
                "prefix value".getBytes(ENCODING_UTF8), 8, "value".getBytes(ENCODING_UTF8)));
    }
    
    @Test
    public void testStartsWithInvalidOffset() throws Exception {
        assertFalse(ByteArrayUtils.startsWith(
                "strA".getBytes(ENCODING_UTF8), Integer.MIN_VALUE, "str".getBytes(ENCODING_UTF8)));
        assertFalse(ByteArrayUtils.startsWith(
                "strA".getBytes(ENCODING_UTF8), Integer.MAX_VALUE, "str".getBytes(ENCODING_UTF8)));
    }
}
