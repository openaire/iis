package eu.dnetlib.iis.wf.metadataextraction.parser;

/**
 * Factory creating {@link ReferenceTextParser} instances based on a parser type name.
 *
 * @author mhorst
 */
public final class ReferenceTextParserFactory {

    public static final String PARSER_CERMINE = "cermine";

    public static final String PARSER_GROBID = "grobid";

    private ReferenceTextParserFactory() {
    }

    /**
     * Creates a {@link ReferenceTextParser} for the given type.
     *
     * @param parserType parser type name ({@value #PARSER_CERMINE} or {@value #PARSER_GROBID})
     * @param grobidServerUrl Grobid server location, required for the Grobid parser
     * @param grobidConnectionTimeout Grobid connection timeout in ms
     * @param grobidReadTimeout Grobid read timeout in ms
     * @return configured parser instance
     */
    public static ReferenceTextParser create(String parserType, String grobidServerUrl,
            int grobidConnectionTimeout, int grobidReadTimeout) {
        if (PARSER_GROBID.equalsIgnoreCase(parserType)) {
            return new GrobidReferenceTextParser(grobidServerUrl, grobidConnectionTimeout, grobidReadTimeout);
        }
        return new CermineReferenceTextParser();
    }
}
