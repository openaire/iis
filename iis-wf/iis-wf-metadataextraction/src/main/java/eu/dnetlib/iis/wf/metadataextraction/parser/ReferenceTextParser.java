package eu.dnetlib.iis.wf.metadataextraction.parser;

import java.util.ArrayList;
import java.util.List;

/**
 * Strategy responsible for parsing a raw bibliographic reference text into
 * structured {@link ParsedReference} fields.
 *
 * @author mhorst
 */
public interface ReferenceTextParser {

    /**
     * Parses a raw bibliographic reference text into structured fields.
     *
     * @param text raw reference text
     * @return parsed fields, or null when the text could not be parsed
     * @throws Exception when parsing fails
     */
    ParsedReference parse(String text) throws Exception;

    /**
     * Parses a list of raw bibliographic reference texts into structured fields.
     * <p>
     * The returned list is aligned with {@code texts} by index: a blank text or
     * an unparseable text yields {@code null}. The default implementation parses
     * each text individually; parsers backed by a remote service (e.g. Grobid)
     * should override this to batch the texts into a single request.
     *
     * @param texts raw reference texts
     * @return parsed fields aligned with {@code texts} by index
     * @throws Exception when parsing fails
     */
    default List<ParsedReference> parse(List<String> texts) throws Exception {
        List<ParsedReference> result = new ArrayList<>(texts.size());
        for (String text : texts) {
            if (ReferenceTextUtils.isBlank(text)) {
                result.add(null);
            } else {
                result.add(parse(text));
            }
        }
        return result;
    }
}
