package eu.dnetlib.iis.wf.metadataextraction.crossref;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.HdfsTestUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.TestWithSharedSparkSession;
import eu.dnetlib.iis.common.spark.avro.AvroDatasetReader;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata;

/**
 * Test class for {@link JsonReferenceParserJob}.
 * 
 * @author mhorst
 */
class JsonReferenceParserJobTest extends TestWithSharedSparkSession {

    private static Path tmpDirRoot;

    @BeforeAll
    static void initWritableTempDirs() throws Exception {
        // Override both java.io.tmpdir and spark.local.dir to use a writable
        // location (the sandbox has /tmp read-only).
        String writableTmp = System.getenv("TMPDIR");
        if (writableTmp != null) {
            tmpDirRoot = Files.createTempDirectory(
                    new java.io.File(writableTmp).toPath(), "jsonRefParserRoot");
            System.setProperty("java.io.tmpdir", tmpDirRoot.toString());
            System.setProperty("spark.local.dir", tmpDirRoot.toString());
        } else {
            tmpDirRoot = new java.io.File(System.getProperty("java.io.tmpdir")).toPath();
        }
    }

    private static final String COUNTER_DOCUMENTS = "processing.crossref.referenceParser.documents";
    
    private static final String COUNTER_REFERENCES = "processing.crossref.referenceParser.references";
    
    private static final String DEFAULT_EXTRACTED_BY = "crossrefBibrefParser";

    // ---------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------

    @Test
    @DisplayName("Job groups references by id and maps all JSON fields correctly")
    void testJobGroupsReferencesAndMapsFields() throws Exception {
        // given
        Path workingDir = createTempDir("jsonRefParserTest");
        Path inputDir = workingDir.resolve("input");
        Path outputDir = workingDir.resolve("output");
        Path outputReportDir = workingDir.resolve("output_report");

        // Two JSON records sharing the same id → one ExtractedDocumentMetadata with two references
        String jsonLines = ""
                + "{\"id\":\"pub1\",\"ref\":{"
                + "\"unstructured\":\"Vidačak, I., & Škrabalo, M. (2014). Exploring the Effects of Europeanization on the Openness of Public Administration in Croatia. Hrvatska i komparativna javna uprava/Croatian and Comparative Public Administration, 14(1), 149–187.\","
                + "\"article-title\":\"Exploring the Effects of Europeanization\","
                + "\"first-page\":\"149\","
                + "\"issue\":\"1\","
                + "\"journal-title\":\"Hrvatska i komparativna javna uprava/Croatian and Comparative Public Administration\","
                + "\"volume\":\"14\","
                + "\"year\":\"2014\""
                + "}}\n"
                + "{\"id\":\"pub1\",\"ref\":{"
                + "\"unstructured\":\"Druge, A. (2020). Some Other Article. Some Journal, 5(2), 55-78.\","
                + "\"article-title\":\"Some Other Article\","
                + "\"edition\":\"2nd\","
                + "\"first-page\":\"55\","
                + "\"issue\":\"2\","
                + "\"journal-title\":\"Some Journal\","
                + "\"volume\":\"5\","
                + "\"year\":\"2020\","
                + "\"series-title\":\"Series A\","
                + "\"type\":\"journal-article\""
                + "}}\n";

        writeGzippedJson(inputDir.resolve("records.json.gz"), jsonLines);

        // when
        JsonReferenceParserJob.main(new String[]{
                "-sharedSparkSession",
                "-inputPath", inputDir.toString(),
                "-outputPath", outputDir.toString(),
                "-outputReportPath", outputReportDir.toString()
        });

        // then
        List<ExtractedDocumentMetadata> results = new AvroDatasetReader(spark())
                .read(outputDir.toString(), ExtractedDocumentMetadata.SCHEMA$, ExtractedDocumentMetadata.class)
                .collectAsList();

        assertEquals(1, results.size(), "expected exactly one ExtractedDocumentMetadata for pub1");

        ExtractedDocumentMetadata doc = results.get(0);
        assertEquals("pub1", doc.getId().toString());
        assertEquals(DEFAULT_EXTRACTED_BY, doc.getExtractedBy().toString());
        assertNull(doc.getTitle(), "title should be empty (not mapped from JSON)");
        assertNull(doc.getAbstract$(), "abstract should be empty");
        assertNull(doc.getLanguage(), "language should be empty");
        assertNull(doc.getKeywords(), "keywords should be empty");
        assertNull(doc.getExternalIdentifiers(), "externalIdentifiers should be empty");
        assertNull(doc.getJournal(), "journal should be empty");
        assertNull(doc.getYear(), "year should be empty");
        assertNull(doc.getPublisher(), "publisher should be empty");
        assertNull(doc.getAuthors(), "authors should be empty");
        assertNull(doc.getAffiliations(), "affiliations should be empty");
        assertNull(doc.getVolume(), "volume should be empty");
        assertNull(doc.getIssue(), "issue should be empty");
        assertNull(doc.getPages(), "pages should be empty");

        // verify references
        assertNotNull(doc.getReferences());
        assertEquals(2, doc.getReferences().size());

        // --- Reference 1 ---
        ReferenceMetadata ref0 = doc.getReferences().get(0);
        assertEquals(ref0.getText().toString(),
                "Vidačak, I., & Škrabalo, M. (2014). Exploring the Effects of Europeanization on the Openness of Public Administration in Croatia. Hrvatska i komparativna javna uprava/Croatian and Comparative Public Administration, 14(1), 149–187.");
        assertNull(ref0.getPosition(), "position should be null (unknown ordering)");
        ReferenceBasicMetadata basic0 = ref0.getBasicMetadata();
        assertNotNull(basic0);
        assertEquals("Exploring the Effects of Europeanization", basic0.getTitle().toString());
        assertEquals("149", basic0.getPages().getStart().toString());
        assertNull(basic0.getPages().getEnd(), "pages.end should be null when only first-page is set");
        assertEquals("1", basic0.getIssue().toString());
        assertEquals("Hrvatska i komparativna javna uprava/Croatian and Comparative Public Administration",
                basic0.getJournal().toString());
        assertEquals("14", basic0.getVolume().toString());
        assertEquals("2014", basic0.getYear().toString());
        assertNull(basic0.getEdition(), "edition not set for ref0");
        assertNull(basic0.getSeries(), "series not set for ref0");
        assertNull(basic0.getType(), "type not set for ref0");

        // --- Reference 2 ---
        ReferenceMetadata ref1 = doc.getReferences().get(1);
        assertEquals("Druge, A. (2020). Some Other Article. Some Journal, 5(2), 55-78.",
                ref1.getText().toString());
        assertNull(ref1.getPosition(), "position should be null");
        ReferenceBasicMetadata basic1 = ref1.getBasicMetadata();
        assertNotNull(basic1);
        assertEquals("Some Other Article", basic1.getTitle().toString());
        assertEquals("2nd", basic1.getEdition().toString());
        assertEquals("55", basic1.getPages().getStart().toString());
        assertEquals("2", basic1.getIssue().toString());
        assertEquals("Some Journal", basic1.getJournal().toString());
        assertEquals("5", basic1.getVolume().toString());
        assertEquals("2020", basic1.getYear().toString());
        assertEquals("Series A", basic1.getSeries().toString());
        assertEquals("journal-article", basic1.getType().toString());

        // verify report
        assertTrue(HdfsTestUtils.countFiles(spark().sparkContext().hadoopConfiguration(),
                outputReportDir.toString(), DataStore.AVRO_FILE_EXT) > 0);
        List<ReportEntry> report = new AvroDatasetReader(spark())
                .read(outputReportDir.toString(), ReportEntry.SCHEMA$, ReportEntry.class)
                .collectAsList();
        assertEquals(2, report.size());
        assertTrue(report.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_DOCUMENTS, 1)));
        assertTrue(report.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_REFERENCES, 2)));
    }

    @Test
    @DisplayName("ISBN without explicit type defaults to 'book'")
    void testIsbnDefaultsToBookType() throws Exception {
        // given
        Path workingDir = createTempDir("jsonRefParserTest_isbn");
        Path inputDir = workingDir.resolve("input");
        Path outputDir = workingDir.resolve("output");
        Path outputReportDir = workingDir.resolve("output_report");

        String jsonLines = ""
                + "{\"id\":\"book1\",\"ref\":{"
                + "\"ISBN\":\"978-3-16-148410-0\","
                + "\"unstructured\":\"A book with ISBN but no type.\""
                + "}}\n";

        writeGzippedJson(inputDir.resolve("records.json.gz"), jsonLines);

        // when
        JsonReferenceParserJob.main(new String[]{
                "-sharedSparkSession",
                "-inputPath", inputDir.toString(),
                "-outputPath", outputDir.toString(),
                "-outputReportPath", outputReportDir.toString()
        });

        // then
        List<ExtractedDocumentMetadata> results = new AvroDatasetReader(spark())
                .read(outputDir.toString(), ExtractedDocumentMetadata.SCHEMA$, ExtractedDocumentMetadata.class)
                .collectAsList();

        assertEquals(1, results.size());
        ReferenceMetadata ref = results.get(0).getReferences().get(0);
        assertEquals("book", ref.getBasicMetadata().getType().toString());
        assertEquals("978-3-16-148410-0",
                ref.getBasicMetadata().getExternalIds().get("ISBN").toString());
    }

    @Test
    @DisplayName("External identifiers are mapped from DOI, ISSN, ISBN fields")
    void testExternalIdentifiersMapping() throws Exception {
        // given
        Path workingDir = createTempDir("jsonRefParserTest_ext");
        Path inputDir = workingDir.resolve("input");
        Path outputDir = workingDir.resolve("output");
        Path outputReportDir = workingDir.resolve("output_report");

        String jsonLines = ""
                + "{\"id\":\"ext1\",\"ref\":{"
                + "\"DOI\":\"10.1000/example\","
                + "\"ISSN\":\"1234-5678\","
                + "\"ISBN\":\"978-0-123-45678-9\","
                + "\"unstructured\":\"Reference with external IDs.\""
                + "}}\n";

        writeGzippedJson(inputDir.resolve("records.json.gz"), jsonLines);

        // when
        JsonReferenceParserJob.main(new String[]{
                "-sharedSparkSession",
                "-inputPath", inputDir.toString(),
                "-outputPath", outputDir.toString(),
                "-outputReportPath", outputReportDir.toString()
        });

        // then
        List<ExtractedDocumentMetadata> results = new AvroDatasetReader(spark())
                .read(outputDir.toString(), ExtractedDocumentMetadata.SCHEMA$, ExtractedDocumentMetadata.class)
                .collectAsList();

        assertEquals(1, results.size());
        ReferenceBasicMetadata basic = results.get(0).getReferences().get(0).getBasicMetadata();
        assertNotNull(basic.getExternalIds());
        assertEquals("10.1000/example", basic.getExternalIds().get("doi").toString());
        assertEquals("1234-5678", basic.getExternalIds().get("ISSN").toString());
        assertEquals("978-0-123-45678-9", basic.getExternalIds().get("ISBN").toString());
    }

    @Test
    @DisplayName("Blank fields are skipped in output")
    void testBlankFieldsAreSkipped() throws Exception {
        // given
        Path workingDir = createTempDir("jsonRefParserTest_blank");
        Path inputDir = workingDir.resolve("input");
        Path outputDir = workingDir.resolve("output");
        Path outputReportDir = workingDir.resolve("output_report");

        String jsonLines = ""
                + "{\"id\":\"min1\",\"ref\":{"
                + "\"unstructured\":\"xyzzy\""
                + "}}\n";

        writeGzippedJson(inputDir.resolve("records.json.gz"), jsonLines);

        // when
        JsonReferenceParserJob.main(new String[]{
                "-sharedSparkSession",
                "-inputPath", inputDir.toString(),
                "-outputPath", outputDir.toString(),
                "-outputReportPath", outputReportDir.toString()
        });

        // then
        List<ExtractedDocumentMetadata> results = new AvroDatasetReader(spark())
                .read(outputDir.toString(), ExtractedDocumentMetadata.SCHEMA$, ExtractedDocumentMetadata.class)
                .collectAsList();

        assertEquals(1, results.size());
        assertEquals("xyzzy", results.get(0).getReferences().get(0).getText().toString());
        ReferenceBasicMetadata basic = results.get(0).getReferences().get(0).getBasicMetadata();
        assertNotNull(basic);
        assertNull(basic.getTitle(), "title should be null");
        assertNull(basic.getAuthors(), "authors should be null");
        assertNull(basic.getPages(), "pages should be null");
        assertNull(basic.getSource(), "source should be null");
        assertNull(basic.getVolume(), "volume should be null");
        assertNull(basic.getYear(), "year should be null");
        assertNull(basic.getJournal(), "journal should be null");
        assertNull(basic.getIssue(), "issue should be null");
        assertNull(basic.getType(), "type should be null");
        assertNull(basic.getExternalIds(), "externalIds should be null");
    }

    @Test
    @DisplayName("Custom extractedBy parameter is respected")
    void testCustomExtractedBy() throws Exception {
        // given
        Path workingDir = createTempDir("jsonRefParserTest_extBy");
        Path inputDir = workingDir.resolve("input");
        Path outputDir = workingDir.resolve("output");
        Path outputReportDir = workingDir.resolve("output_report");

        String jsonLines = ""
                + "{\"id\":\"cust1\",\"ref\":{\"unstructured\":\"Custom extracted by test.\"}}\n";

        writeGzippedJson(inputDir.resolve("records.json.gz"), jsonLines);

        // when
        JsonReferenceParserJob.main(new String[]{
                "-sharedSparkSession",
                "-inputPath", inputDir.toString(),
                "-outputPath", outputDir.toString(),
                "-outputReportPath", outputReportDir.toString(),
                "-extractedBy", "myCustomParser"
        });

        // then
        List<ExtractedDocumentMetadata> results = new AvroDatasetReader(spark())
                .read(outputDir.toString(), ExtractedDocumentMetadata.SCHEMA$, ExtractedDocumentMetadata.class)
                .collectAsList();

        assertEquals(1, results.size());
        assertEquals("myCustomParser", results.get(0).getExtractedBy().toString());
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    /**
     * Creates a temporary directory under a writable location (bypassing
     * the sandbox's read-only /tmp).
     */
    private static Path createTempDir(String prefix) throws Exception {
        Path parent = tmpDirRoot != null ? tmpDirRoot : Path.of(System.getProperty("java.io.tmpdir"));
        Path dir = Files.createTempDirectory(parent, prefix);
        dir.toFile().deleteOnExit();
        return dir;
    }

    /**
     * Writes a string of JSON lines to a gzip-compressed file,
     * creating parent directories if needed.
     */
    private static void writeGzippedJson(Path filePath, String jsonLines) throws Exception {
        Files.createDirectories(filePath.getParent());
        try (Writer writer = new OutputStreamWriter(
                new GZIPOutputStream(new FileOutputStream(filePath.toFile())),
                StandardCharsets.UTF_8)) {
            writer.write(jsonLines);
            writer.flush();
        }
    }
}
