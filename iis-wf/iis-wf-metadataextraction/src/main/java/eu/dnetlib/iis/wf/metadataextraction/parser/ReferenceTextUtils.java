package eu.dnetlib.iis.wf.metadataextraction.parser;

/**
 * Utilities for handling raw bibliographic reference text.
 *
 * @author mhorst
 */
public final class ReferenceTextUtils {

    private ReferenceTextUtils() {
    }

    /**
     * Returns true when the given text contains no meaningful characters,
     * treating null, empty, whitespace-only and Unicode invisible/space-only
     * text (e.g. no-break space, zero-width space, BOM) as blank.
     * <p>
     * {@link org.apache.commons.lang3.StringUtils#isBlank} (which relies on
     * {@link Character#isWhitespace(char)}) misses several Unicode space and
     * zero-width characters (e.g. {@code \u00A0}, {@code \u2007}, {@code \u202F},
     * {@code \u200B}, {@code \uFEFF}). Sending such text to Grobid results in an
     * empty citation which the server rejects with HTTP 500.
     *
     * @param text text to check
     * @return true when the text is effectively blank
     */
    public static boolean isBlank(String text) {
        if (text == null) {
            return true;
        }
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isWhitespace(c)) {
                continue;
            }
            if (isInvisibleChar(c)) {
                continue;
            }
            return false;
        }
        return true;
    }

    /**
     * Characters that render as invisible/space but are not recognized by
     * {@link Character#isWhitespace(char)} (Java deliberately excludes the
     * non-breaking space variants and does not cover zero-width characters).
     */
    private static boolean isInvisibleChar(char c) {
        return c == 0x00A0     // no-break space
                || c == 0x2007 // figure space
                || c == 0x202F // narrow no-break space
                || c == 0x200B // zero-width space
                || c == 0x200C // zero-width non-joiner
                || c == 0x200D // zero-width joiner
                || c == 0x2060 // word joiner
                || c == 0xFEFF;// zero-width no-break space / BOM
    }
}
