package eu.dnetlib.iis.wf.metadataextraction.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test class for the reference-text omission contract of {@link ReferenceTextParser}.
 *
 * @author mhorst
 */
class ReferenceTextParserOmissionTest {

    private static final String VALID_REFERENCE =
            "Philpott, W.H. and M.D. (2001). The Significance of Selective Food. Journal 5(1), 1-10.";

    @Test
    @DisplayName("Default list parsing omits references which are blank or too short")
    void testDefaultListParsingOmitsTooShortReferences() throws Exception {
        // given
        RecordingParser parser = new RecordingParser();

        // when - a blank text is not an intuitive argument for 'blank or too short'
        List<ParsedReference> result = parser.parse(java.util.Arrays.asList(
                "",                    // blank
                "\u00A0\u200B",        // blank (Unicode invisible only)
                ".",                   // too short
                "..",                  // too short
                VALID_REFERENCE));     // parseable

        // then - only the valid reference was handed over to the parser
        assertEquals(1, parser.invokedTexts.size(),
                "only the valid reference should be parsed, got: " + parser.invokedTexts);
        assertEquals(VALID_REFERENCE, parser.invokedTexts.get(0));

        // and the result stays aligned with the input, omitted entries yielding null
        assertEquals(5, result.size());
        assertNull(result.get(0));
        assertNull(result.get(1));
        assertNull(result.get(2));
        assertNull(result.get(3));
        assertNotNull(result.get(4), "the valid reference should have been parsed");
    }

    @Test
    @DisplayName("Concrete parsers return null for references which are blank or too short")
    void testSingleParsingOmitsTooShortReference() throws Exception {
        // given - a Grobid parser pointing at an unroutable endpoint: if an omitted
        // text were sent, the call would fail to connect (proving no request is made)
        CermineReferenceTextParser cermine = new CermineReferenceTextParser();
        GrobidReferenceTextParser grobid =
                new GrobidReferenceTextParser("http://localhost:1", 1000, 1000);

        // when / then - blank and too short texts are omitted locally, without parsing
        assertNull(cermine.parse("\u00A0"));
        assertNull(cermine.parse("."));
        assertNull(cermine.parse(".."));
        assertNull(grobid.parse("\u00A0"));
        assertNull(grobid.parse("."));
        assertNull(grobid.parse(".."));
    }

    /**
     * {@link ReferenceTextParser} test double recording which texts actually reach
     * the parser implementation.
     */
    private static class RecordingParser implements ReferenceTextParser {

        private final List<String> invokedTexts = new ArrayList<>();

        @Override
        public ParsedReference parse(String text) throws Exception {
            invokedTexts.add(text);
            ParsedReference parsed = new ParsedReference();
            parsed.setTitle(text);
            return parsed;
        }
    }

    @Test
    @DisplayName("Minimum reference length is a sane, non-zero threshold")
    void testMinimumReferenceLength() {
        assertTrue(ReferenceTextUtils.MIN_REFERENCE_LENGTH > 1);
        assertFalse(ReferenceTextUtils.isOmitted("x".repeat(ReferenceTextUtils.MIN_REFERENCE_LENGTH)));
    }
}
