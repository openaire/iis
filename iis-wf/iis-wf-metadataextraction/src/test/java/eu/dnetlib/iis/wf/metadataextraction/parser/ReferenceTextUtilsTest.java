package eu.dnetlib.iis.wf.metadataextraction.parser;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link ReferenceTextUtils}.
 *
 * @author mhorst
 */
class ReferenceTextUtilsTest {

    @Test
    @DisplayName("Recognizes null, empty and ordinary whitespace as blank")
    void testBlankNullAndWhitespace() {
        assertTrue(ReferenceTextUtils.isBlank(null));
        assertTrue(ReferenceTextUtils.isBlank(""));
        assertTrue(ReferenceTextUtils.isBlank("   "));
        assertTrue(ReferenceTextUtils.isBlank("\t\n "));
    }

    @Test
    @DisplayName("Recognizes Unicode invisible/space-only text as blank")
    void testBlankUnicodeInvisible() {
        assertTrue(ReferenceTextUtils.isBlank("\u00A0"));                    // no-break space
        assertTrue(ReferenceTextUtils.isBlank("\u200B"));                    // zero-width space
        assertTrue(ReferenceTextUtils.isBlank("\u2007"));                    // figure space
        assertTrue(ReferenceTextUtils.isBlank("\u202F"));                    // narrow no-break space
        assertTrue(ReferenceTextUtils.isBlank("\uFEFF"));                    // BOM / ZWNBSP
        assertTrue(ReferenceTextUtils.isBlank(" \u00A0\u200B\uFEFF "));      // mixed invisible
    }

    @Test
    @DisplayName("Recognizes text with meaningful characters as non-blank")
    void testNonBlank() {
        assertFalse(ReferenceTextUtils.isBlank("A"));
        assertFalse(ReferenceTextUtils.isBlank("x"));
        assertFalse(ReferenceTextUtils.isBlank("The Significance of Selective Food"));
        assertFalse(ReferenceTextUtils.isBlank(" \u00A0Philpott\u00A0 "));  // NBSP around real text
    }
}
