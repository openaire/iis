package eu.dnetlib.iis.wf.affextraction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for extracted helper methods of {@link CachedHtmlImportAndExtractionJob}.
 */
class CachedHtmlImportAndExtractionJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static String SAMPLE_JSON;

    @BeforeAll
    static void loadResources() throws IOException {
        SAMPLE_JSON = IOUtils.toString(
                CachedHtmlImportAndExtractionJobTest.class.getResourceAsStream("sample.json"),
                StandardCharsets.UTF_8).trim();
    }

    // -------------------- updateJsonWithId tests --------------------

    @Test
    void updateJsonWithId_shouldReplaceIdField() throws Exception {
        // given
        String newId = "some-new-execution-specific-id";

        // when
        String result = CachedHtmlImportAndExtractionJob.updateJsonWithId(SAMPLE_JSON, newId);

        // then
        JsonNode resultNode = MAPPER.readTree(result);
        assertEquals(newId, resultNode.get("id").asText());
        // verify other fields are preserved
        assertEquals("10.1080/23322039.2025.2460082", resultNode.get("doi").asText());
        assertEquals("6493a55ee9772e7719c3ca15c9e00a7d", resultNode.get("checksum").asText());
        assertTrue(resultNode.get("success").asBoolean());
    }

    @Test
    void updateJsonWithId_shouldHandlePlaceholderId() throws Exception {
        // given: JSON with an id placeholder (as stored in cache)
        String cachedJson = SAMPLE_JSON.replace(
                "\"id\": \"doajarticles::b7c1952e2ade325eb72af12a71b485fc\"",
                "\"id\": \"$ID_PLACEHOLDER$\"");
        String newId = "restored-actual-id";

        // when
        String result = CachedHtmlImportAndExtractionJob.updateJsonWithId(cachedJson, newId);

        // then
        JsonNode resultNode = MAPPER.readTree(result);
        assertEquals(newId, resultNode.get("id").asText());
    }

    // -------------------- prepareCacheEntry tests --------------------

    @Test
    void prepareCacheEntry_shouldExtractChecksumAndReplaceId() throws Exception {
        // when
        DocumentText result = CachedHtmlImportAndExtractionJob.prepareCacheEntry(SAMPLE_JSON);

        // then
        assertEquals("6493a55ee9772e7719c3ca15c9e00a7d", result.getId().toString());

        // verify the text has the id replaced with a placeholder
        JsonNode textNode = MAPPER.readTree(result.getText().toString());
        assertEquals("$ID_PLACEHOLDER$", textNode.get("id").asText());

        // verify checksum is still present in the text
        assertEquals("6493a55ee9772e7719c3ca15c9e00a7d", textNode.get("checksum").asText());

        // verify other fields are preserved
        assertEquals("10.1080/23322039.2025.2460082", textNode.get("doi").asText());
        assertTrue(textNode.get("success").asBoolean());
    }

    @Test
    void prepareCacheEntry_shouldThrowWhenChecksumMissing() {
        // given: JSON without a checksum field
        String jsonWithoutChecksum = "{\"id\": \"some-id\", \"doi\": \"10.1234/test\", \"success\": true}";

        // when & then
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> CachedHtmlImportAndExtractionJob.prepareCacheEntry(jsonWithoutChecksum));
        assertTrue(exception.getMessage().contains("'checksum' field is missing"));
    }
}
