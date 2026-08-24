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

    @Test
    @DisplayName("Parses a bare biblStruct root element (as returned by the 0.8.2 server)")
    void testParseBareBiblStructRoot() throws Exception {
        // given - exact structure returned by grobid.openaire-cloud.icm.edu.pl (0.8.2)
        String tei = ""
                + "<biblStruct >"
                + "        <monogr>"
                + "                <title/>"
                + "                <author>"
                + "                        <persName><forename type=\"first\">K</forename><forename type=\"middle\">G</forename><surname>Kürn</surname></persName>"
                + "                </author>"
                + "                <imprint>"
                + "                        <date type=\"published\" when=\"1955\">1955</date>"
                + "                </imprint>"
                + "        </monogr>"
                + "</biblStruct>";

        // when
        ParsedReference parsed = GrobidTeiBiblStructParser.parse(tei);

        // then
        assertNotNull(parsed);
        assertEquals("K G Kürn", parsed.getAuthors().get(0));
        assertEquals("1955", parsed.getYear());
        assertEquals(1, parsed.getAuthors().size());
    }

    @Test
    @DisplayName("Extracts only the year from YYYY-MM and YYYY-MM-DD dates")
    void testParseYearFromFullDates() throws Exception {
        // given
        String tei = ""
                + "<TEI>"
                + "  <biblStruct>"
                + "    <monogr>"
                + "      <title level=\"j\">Some Journal</title>"
                + "      <imprint>"
                + "        <date type=\"published\" when=\"2014-01\">2014-01</date>"
                + "      </imprint>"
                + "    </monogr>"
                + "  </biblStruct>"
                + "</TEI>";

        // when
        ParsedReference parsed = GrobidTeiBiblStructParser.parse(tei);

        // then - YYYY-MM -> year only
        assertNotNull(parsed);
        assertEquals("2014", parsed.getYear());
    }

    @Test
    @DisplayName("Extracts only the year from a YYYY-MM-DD date")
    void testParseYearFromFullDateWithDay() throws Exception {
        // given
        String tei = ""
                + "<biblStruct>"
                + "  <monogr>"
                + "    <title level=\"j\">Some Journal</title>"
                + "    <imprint>"
                + "      <date type=\"published\" when=\"2014-01-15\">2014-01-15</date>"
                + "    </imprint>"
                + "  </monogr>"
                + "</biblStruct>";

        // when
        ParsedReference parsed = GrobidTeiBiblStructParser.parse(tei);

        // then - YYYY-MM-DD -> year only
        assertNotNull(parsed);
        assertEquals("2014", parsed.getYear());
    }

    @Test
    @DisplayName("Parses a listBibl response with multiple biblStructs, preserving order")
    void testParseList() throws Exception {
        // given - TEI envelope as returned by /api/processCitationList (0.8.2)
        String tei = ""
                + "<TEI xmlns=\"http://www.tei-c.org/ns/1.0\" xmlns:xlink=\"http://www.w3.org/1999/xlink\">"
                + "<teiHeader/><text><front/><body/><back><div><listBibl>"
                + "<biblStruct><monogr><title level=\"j\">Journal A</title>"
                + "<imprint><date type=\"published\" when=\"2010\">2010</date></imprint></monogr></biblStruct>"
                + "<biblStruct><monogr><title level=\"j\">Journal B</title>"
                + "<author><persName><forename type=\"first\">A</forename><surname>Author</surname></persName></author>"
                + "<imprint><date type=\"published\" when=\"2014-01-15\">2014-01-15</date></imprint></monogr></biblStruct>"
                + "</listBibl></div></back></text></TEI>";

        // when
        List<ParsedReference> parsed = GrobidTeiBiblStructParser.parseList(tei);

        // then
        assertNotNull(parsed);
        assertEquals(2, parsed.size());
        assertEquals("Journal A", parsed.get(0).getJournal());
        assertEquals("2010", parsed.get(0).getYear());
        assertEquals("Journal B", parsed.get(1).getJournal());
        assertEquals("A Author", parsed.get(1).getAuthors().get(0));
        assertEquals("2014", parsed.get(1).getYear(), "year extracted from YYYY-MM-DD");
    }

    @Test
    @DisplayName("Ignores a leading UTF-8 BOM in the response (fixes 'Content is not allowed in prolog')")
    void testParseIgnoresLeadingBom() throws Exception {
        // given - TEI prefixed with a UTF-8 BOM character
        String tei = "\uFEFF" + "<biblStruct><monogr><title level=\"j\">Bom Journal</title>"
                + "<imprint><date type=\"published\" when=\"2012\">2012</date></imprint></monogr></biblStruct>";

        // when
        ParsedReference parsed = GrobidTeiBiblStructParser.parse(tei);

        // then
        assertNotNull(parsed);
        assertEquals("Bom Journal", parsed.getJournal());
        assertEquals("2012", parsed.getYear());
    }
}
