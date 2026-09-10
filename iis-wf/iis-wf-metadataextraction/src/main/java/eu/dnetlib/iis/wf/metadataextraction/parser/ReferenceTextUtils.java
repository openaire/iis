package eu.dnetlib.iis.wf.metadataextraction.parser;

/**
 * Utilities for handling raw bibliographic reference text.
 *
 * @author mhorst
 */
public final class ReferenceTextUtils {

    /**
     * Minimum number of meaningful characters a raw reference text must contain to be
     * worth parsing. Shorter texts (e.g. a single dot) cannot yield any bibliographic
     * data, so they are omitted from processing.
     */
    public static final int MIN_REFERENCE_LENGTH = 3;

    private ReferenceTextUtils() {
    }

    /**
     * Returns true when the given text is meant to be omitted from parsing, either
     * because it is blank or because it is too short to carry any bibliographic
     * information (fewer than {@value #MIN_REFERENCE_LENGTH} meaningful characters,
     * e.g. a lone dot).
     *
     * @param text text to check
     * @return true when the text should not be sent to a reference parser
     */
    public static boolean isOmitted(String text) {
        return !hasAtLeastMeaningfulChars(text, MIN_REFERENCE_LENGTH);
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
        return !hasAtLeastMeaningfulChars(text, 1);
    }

    /**
     * Counts meaningful characters (i.e. excluding whitespace and Unicode
     * invisible/space-only characters) but stops as soon as {@code required} of
     * them have been found.
     * <p>
     * This keeps the common case cheap: a typical reference proves it is not
     * blank/too short within its first few characters, so the scan is bounded by
     * the position of the last needed character rather than by the length of the
     * text. Only texts made of whitespace/invisible characters (typically very
     * short) are scanned in full.
     *
     * @param text text to inspect
     * @param required number of meaningful characters to look for
     * @return true when the text holds at least {@code required} meaningful characters
     */
    private static boolean hasAtLeastMeaningfulChars(String text, int required) {
        if (text == null) {
            return false;
        }
        int found = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isWhitespace(c) || isInvisibleChar(c)) {
                continue;
            }
            if (++found >= required) {
                return true;
            }
        }
        return false;
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
