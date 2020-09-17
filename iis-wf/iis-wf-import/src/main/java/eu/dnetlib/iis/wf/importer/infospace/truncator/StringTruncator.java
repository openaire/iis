package eu.dnetlib.iis.wf.importer.infospace.truncator;

import java.text.BreakIterator;


public class StringTruncator {

    /**
     * Truncates source strings preserving word boundaries, keeping words that would be broke.
     *
     * @param source String to be truncated.
     * @param length Length of the source string to keep.
     * @return Truncated string.
     */
    public static String truncateWithoutWordSplit(String source, int length) {
        if (source.length() <= length) {
            return source;
        }

        BreakIterator atWordInstance = BreakIterator.getWordInstance();
        atWordInstance.setText(source);
        return source.substring(0, atWordInstance.following(length)).trim();
    }
}
