package eu.dnetlib.iis.wf.metadataextraction.grobid;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import eu.dnetlib.iis.wf.metadataextraction.parser.ParsedReference;

/**
 * Test class for {@link GrobidTeiBiblStructParser}.
 *
 * @author mhorst
 */
class GrobidTeiBiblStructParserTest {

    private static final String SAMPLE_TEI = ""
            + "<TEI xmlns=\"http://www.tei-c.org/ns/1.0\">"
            + "  <teiHeader/>"
            + "  <text>"
            + "    <back>"
            + "      <listBibl>"
            + "        <biblStruct>"
            + "          <analytic>"
            + "            <title level=\"a\" type=\"main\">Bad is stronger than good</title>"
            + "            <author><persName><forename type=\"first\">R</forename><forename type=\"middle\">F</forename><surname>Baumeister</surname></persName></author>"
            + "            <author><persName><forename type=\"first\">E</forename><surname>Bratslavsky</surname></persName></author>"
            + "            <idno type=\"DOI\">10.1037//1089-2680.5.4.323</idno>"
            + "          </analytic>"
            + "          <monogr>"
            + "            <title level=\"j\">Review of General Psychology</title>"
            + "            <imprint>"
            + "              <biblScope unit=\"volume\">5</biblScope>"
            + "              <biblScope unit=\"page\" from=\"323\" to=\"370\"/>"
            + "              <date type=\"published\" when=\"2001\">2001</date>"
            + "              <publisher>American Psychological Association</publisher>"
            + "              <pubPlace>Washington, DC</pubPlace>"
            + "            </imprint>"
            + "          </monogr>"
            + "        </biblStruct>"
            + "      </listBibl>"
            + "    </back>"
            + "  </text>"
            + "</TEI>";

    @Test
    @DisplayName("Parses a Grobid biblStruct into ParsedReference fields")
    void testParse() throws Exception {
        // given / when
        ParsedReference parsed = GrobidTeiBiblStructParser.parse(SAMPLE_TEI);

        // then
        assertNotNull(parsed);
        assertEquals("Bad is stronger than good", parsed.getTitle());
        assertEquals(Arrays.asList("R F Baumeister", "E Bratslavsky"), parsed.getAuthors());
        assertEquals("Review of General Psychology", parsed.getJournal());
        assertEquals("5", parsed.getVolume());
        assertEquals("323-370", parsed.getPages());
        assertEquals("2001", parsed.getYear());
        assertEquals("American Psychological Association", parsed.getPublisher());
        assertEquals("Washington, DC", parsed.getLocation());
        assertEquals("10.1037//1089-2680.5.4.323", parsed.getDoi());
    }

    @Test
    @DisplayName("Parses TEI without the TEI namespace (local-name based extraction)")
    void testParseWithoutNamespace() throws Exception {
        // given - same structure but no xmlns declaration
        String teiWithoutNamespace = SAMPLE_TEI.replace("xmlns=\"http://www.tei-c.org/ns/1.0\"", "");

        // when
        ParsedReference parsed = GrobidTeiBiblStructParser.parse(teiWithoutNamespace);

        // then
        assertNotNull(parsed);
        assertEquals("Bad is stronger than good", parsed.getTitle());
        assertEquals("Review of General Psychology", parsed.getJournal());
        assertEquals("323-370", parsed.getPages());
    }

    @Test
    @DisplayName("Returns null when no biblStruct is present")
    void testParseNoBiblStruct() throws Exception {
        // given / when
        ParsedReference parsed = GrobidTeiBiblStructParser.parse("<TEI><teiHeader/></TEI>");

        // then
        assertNull(parsed);
    }

    @Test
    @DisplayName("Extracts ISBN, ISSN, edition, series and url when present")
    void testParseFullBiblStruct() throws Exception {
        // given
        String tei = ""
                + "<TEI>"
                + "  <biblStruct>"
                + "    <monogr>"
                + "      <title level=\"m\" type=\"main\">Some Handbook</title>"
                + "      <edition>2nd ed.</edition>"
                + "      <imprint>"
                + "        <date type=\"published\" when=\"2010\">2010</date>"
                + "        <publisher>Springer</publisher>"
                + "        <pubPlace>Berlin</pubPlace>"
                + "      </imprint>"
                + "      <series><title>Lecture Notes in Computer Science</title></series>"
                + "      <idno type=\"ISBN\">978-3-540-00000-0</idno>"
                + "      <idno type=\"ISSN\">0302-9743</idno>"
                + "    </monogr>"
                + "    <ptr target=\"https://example.com/handbook\"/>"
                + "  </biblStruct>"
                + "</TEI>";

        // when
        ParsedReference parsed = GrobidTeiBiblStructParser.parse(tei);

        // then
        assertNotNull(parsed);
        assertEquals("Some Handbook", parsed.getTitle());
        assertEquals("Some Handbook", parsed.getJournal());
        assertEquals("2nd ed.", parsed.getEdition());
        assertEquals("2010", parsed.getYear());
        assertEquals("Springer", parsed.getPublisher());
        assertEquals("Berlin", parsed.getLocation());
        assertEquals("Lecture Notes in Computer Science", parsed.getSeries());
        assertEquals("978-3-540-00000-0", parsed.getIsbn());
        assertEquals("0302-9743", parsed.getIssn());
        assertEquals("https://example.com/handbook", parsed.getUrl());
    }

    @Test
    @DisplayName("Returns empty author list when no authors are present")
    void testParseWithoutAuthors() throws Exception {
        // given
        String tei = ""
                + "<TEI>"
                + "  <biblStruct>"
                + "    <monogr>"
                + "      <title level=\"m\" type=\"main\">An Anonymous Work</title>"
                + "    </monogr>"
                + "  </biblStruct>"
                + "</TEI>";

        // when
        ParsedReference parsed = GrobidTeiBiblStructParser.parse(tei);

        // then
        assertNotNull(parsed);
        assertEquals("An Anonymous Work", parsed.getTitle());
        List<String> authors = parsed.getAuthors();
        assertNotNull(authors);
        assertEquals(0, authors.size());
    }
}
