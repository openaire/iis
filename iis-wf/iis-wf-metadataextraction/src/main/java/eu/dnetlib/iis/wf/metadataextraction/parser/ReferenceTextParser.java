package eu.dnetlib.iis.wf.metadataextraction.parser;

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
}
