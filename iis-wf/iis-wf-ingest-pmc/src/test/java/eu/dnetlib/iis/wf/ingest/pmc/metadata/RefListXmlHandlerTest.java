package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ReferenceBasicMetadata;
import eu.dnetlib.iis.ingest.pmc.metadata.schemas.ReferenceMetadata;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.xml.sax.InputSource;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RefListXmlHandlerTest {

    private SAXParser saxParser;
    private ExtractedDocumentMetadata.Builder builder;
    private RefListXmlHandler refListXmlHandler;

    @BeforeEach
    public void init() throws Exception {
        SAXParserFactory saxFactory = SAXParserFactory.newInstance();
        saxFactory.setValidating(false);
        saxParser = saxFactory.newSAXParser();
        builder = ExtractedDocumentMetadata.newBuilder();
        builder.setId("id");
        builder.setEntityType("entity-type");
        builder.setText("text");
        refListXmlHandler = new RefListXmlHandler(builder);
    }

    @Test
    @DisplayName("References with 'nlm-citation' element are parsed")
    public void givenRefListXmlHandler_whenRefWithNlmCitationIsParsed_thenReferenceMetadataWithAllDataIsExtracted() throws Exception {
        String input = "<ref content-type=\"article\" id=\"B1\"><label>1</label><nlm-citation publication-type=\"journal\"><person-group person-group-type=\"author\"><name><surname>McCandlish</surname><given-names>R.</given-names></name><name><surname>Bowler</surname><given-names>U.</given-names></name><name><surname>van Asten</surname><given-names>H.</given-names></name><name><surname>Berridge</surname><given-names>G.</given-names></name><name><surname>Winter</surname><given-names>C.</given-names></name><name><surname>Sames</surname><given-names>L.</given-names></name><name><surname>Garcia</surname><given-names>J.</given-names></name><name><surname>Renfrew</surname><given-names>M.</given-names></name><name><surname>Elbourne</surname><given-names>D.</given-names></name></person-group><article-title>A randomised controlled trial of care of the perineum during second stage of normal labour</article-title><source><italic>British Journal of Obstetrics and Gynaecology</italic></source><year>1998</year><volume>105</volume><issue>12</issue><fpage>1262</fpage><lpage>1272</lpage><pub-id pub-id-type=\"doi\">10.1111/j.1471-0528.1998.tb10004.x</pub-id><pub-id pub-id-type=\"other\">2-s2.0-0032423003</pub-id></nlm-citation></ref>";

        InputSource inputSource = new InputSource(IOUtils.toInputStream(input, StandardCharsets.UTF_8));
        saxParser.parse(inputSource, refListXmlHandler);

        List<ReferenceMetadata> references = builder.build().getReferences();

        assertEquals(1, references.size());
        assertForReferenceMetadata("A randomised controlled trial of care of the perineum during second stage of normal labour",
                Arrays.asList("McCandlish, R.",
                        "Bowler, U.",
                        "van Asten, H.",
                        "Berridge, G.",
                        "Winter, C.",
                        "Sames, L.",
                        "Garcia, J.",
                        "Renfrew, M.",
                        "Elbourne, D."),
                "1262",
                "1272",
                "British Journal of Obstetrics and Gynaecology",
                "105",
                "1998",
                "12",
                Stream.of(
                        new AbstractMap.SimpleImmutableEntry<>("other", "2-s2.0-0032423003"),
                        new AbstractMap.SimpleImmutableEntry<>("doi", "10.1111/j.1471-0528.1998.tb10004.x")
                ).collect(Collectors.toMap(AbstractMap.SimpleImmutableEntry::getKey, AbstractMap.SimpleImmutableEntry::getValue)),
                1,
                "McCandlish, R., Bowler, U., van Asten, H., Berridge, G., Winter, C., Sames, L., Garcia, J., Renfrew, M., Elbourne, D.. A randomised controlled trial of care of the perineum during second stage of normal labour. British Journal of Obstetrics and Gynaecology. 1998; 105 (12): 1262-1272"
                , references.get(0));
    }

    private static void assertForReferenceMetadata(CharSequence title,
                                                   List<CharSequence> authors,
                                                   CharSequence pagesStart,
                                                   CharSequence pagesEnd,
                                                   CharSequence source,
                                                   CharSequence volume,
                                                   CharSequence year,
                                                   CharSequence issue,
                                                   Map<CharSequence, CharSequence> externalIds,
                                                   Integer position,
                                                   CharSequence text,
                                                   ReferenceMetadata referenceMetadata) {
        assertForBasicMetadata(title, authors, pagesStart, pagesEnd, source, volume, year, issue, externalIds,
                referenceMetadata.getBasicMetadata());
        assertEquals(position, referenceMetadata.getPosition());
        assertEquals(text, referenceMetadata.getText());
    }

    private static void assertForBasicMetadata(CharSequence title,
                                               List<CharSequence> authors,
                                               CharSequence pagesStart,
                                               CharSequence pagesEnd,
                                               CharSequence source,
                                               CharSequence volume,
                                               CharSequence year,
                                               CharSequence issue,
                                               Map<CharSequence, CharSequence> externalIds,
                                               ReferenceBasicMetadata referenceBasicMetadata) {
        assertEquals(title, referenceBasicMetadata.getTitle());
        assertEquals(authors, referenceBasicMetadata.getAuthors());
        assertEquals(pagesStart, referenceBasicMetadata.getPages().getStart());
        assertEquals(pagesEnd, referenceBasicMetadata.getPages().getEnd());
        assertEquals(source, referenceBasicMetadata.getSource());
        assertEquals(volume, referenceBasicMetadata.getVolume());
        assertEquals(year, referenceBasicMetadata.getYear());
        assertEquals(issue, referenceBasicMetadata.getIssue());
        assertEquals(externalIds.size(), referenceBasicMetadata.getExternalIds().size());
        externalIds.forEach((key, value) -> assertThat(referenceBasicMetadata.getExternalIds(), hasEntry(key, value)));
    }
}