package eu.dnetlib.iis.wf.metadataextraction.crossref;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.cache.CacheMetadataManagingProcess;
import eu.dnetlib.iis.common.cache.CacheStorageUtils;
import eu.dnetlib.iis.common.cache.CacheStorageUtils.CacheRecordType;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.HdfsTestUtils;
import eu.dnetlib.iis.common.lock.HadoopFsLockManagerFactory;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.TestWithSharedSparkSession;
import eu.dnetlib.iis.common.spark.avro.AvroDatasetReader;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
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
    
    private static final String COUNTER_FROMCACHE = "processing.crossref.referenceParser.fromCache.total";
    
    private static final String COUNTER_PROCESSED_TOTAL = "processing.crossref.referenceParser.processed.total";
    
    private static final String COUNTER_PROCESSED_FAULT = "processing.crossref.referenceParser.processed.fault";
    
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
        JsonReferenceParserJob.main(buildJobArgs(workingDir, inputDir, outputDir, outputReportDir));

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
        assertEquals(5, report.size());
        assertTrue(report.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_DOCUMENTS, 1)));
        assertTrue(report.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_REFERENCES, 2)));
        assertTrue(report.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_FROMCACHE, 0)));
        assertTrue(report.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_PROCESSED_TOTAL, 1)));
        assertTrue(report.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_PROCESSED_FAULT, 0)));
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
        JsonReferenceParserJob.main(buildJobArgs(workingDir, inputDir, outputDir, outputReportDir));

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
        JsonReferenceParserJob.main(buildJobArgs(workingDir, inputDir, outputDir, outputReportDir));

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
        JsonReferenceParserJob.main(buildJobArgs(workingDir, inputDir, outputDir, outputReportDir));

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
        JsonReferenceParserJob.main(buildJobArgs(workingDir, inputDir, outputDir, outputReportDir,
                "-extractedBy", "myCustomParser"));

        // then
        List<ExtractedDocumentMetadata> results = new AvroDatasetReader(spark())
                .read(outputDir.toString(), ExtractedDocumentMetadata.SCHEMA$, ExtractedDocumentMetadata.class)
                .collectAsList();

        assertEquals(1, results.size());
        assertEquals("myCustomParser", results.get(0).getExtractedBy().toString());
    }

    @Test
    @DisplayName("Invisible-only unstructured text is not parsed (mapped from explicit fields only)")
    void testInvisibleOnlyUnstructuredIsSkipped() throws Exception {
        // given - unstructured containing only Unicode invisible/space chars (NBSP,
        // zero-width space, BOM) which would otherwise be sent to Grobid as an
        // empty citation and rejected with HTTP 500
        Path workingDir = createTempDir("jsonRefParserTest_invisible");
        Path inputDir = workingDir.resolve("input");
        Path outputDir = workingDir.resolve("output");
        Path outputReportDir = workingDir.resolve("output_report");

        String jsonLines = ""
                + "{\"id\":\"inv1\",\"ref\":{"
                + "\"unstructured\":\"\u00A0\u200B\uFEFF\","
                + "\"article-title\":\"Title Only\","
                + "\"journal-title\":\"Some Journal\""
                + "}}\n";

        writeGzippedJson(inputDir.resolve("records.json.gz"), jsonLines);

        // when
        JsonReferenceParserJob.main(buildJobArgs(workingDir, inputDir, outputDir, outputReportDir));

        // then - the record is produced without exception, with explicit fields only
        List<ExtractedDocumentMetadata> results = new AvroDatasetReader(spark())
                .read(outputDir.toString(), ExtractedDocumentMetadata.SCHEMA$, ExtractedDocumentMetadata.class)
                .collectAsList();

        assertEquals(1, results.size());
        ReferenceBasicMetadata basic = results.get(0).getReferences().get(0).getBasicMetadata();
        assertEquals("Title Only", basic.getTitle().toString());
        assertEquals("Some Journal", basic.getJournal().toString());
        assertNull(results.get(0).getReferences().get(0).getText(),
                "invisible-only text should not be stored as reference text");
    }

    @Test
    @DisplayName("Too short unstructured text is not parsed (mapped from explicit fields only)")
    void testTooShortUnstructuredIsOmitted() throws Exception {
        // given - a reference carrying only a dot, which cannot yield any
        // bibliographic data and would otherwise be sent for parsing
        Path workingDir = createTempDir("jsonRefParserTest_tooShort");
        Path inputDir = workingDir.resolve("input");
        Path outputDir = workingDir.resolve("output");
        Path outputReportDir = workingDir.resolve("output_report");

        String jsonLines = ""
                + "{\"id\":\"short1\",\"ref\":{"
                + "\"unstructured\":\".\","
                + "\"article-title\":\"Title Only\","
                + "\"journal-title\":\"Some Journal\""
                + "}}\n"
                + "{\"id\":\"short2\",\"ref\":{"
                + "\"unstructured\":\"..\","
                + "\"article-title\":\"Another Title\""
                + "}}\n";

        writeGzippedJson(inputDir.resolve("records.json.gz"), jsonLines);

        // when
        JsonReferenceParserJob.main(buildJobArgs(workingDir, inputDir, outputDir, outputReportDir));

        // then - records are produced without parsing the meaningless text,
        // so only the explicitly defined JSON fields are mapped
        List<ExtractedDocumentMetadata> results = new AvroDatasetReader(spark())
                .read(outputDir.toString(), ExtractedDocumentMetadata.SCHEMA$, ExtractedDocumentMetadata.class)
                .collectAsList();

        assertEquals(2, results.size());

        Map<String, ExtractedDocumentMetadata> byId = new HashMap<>();
        for (ExtractedDocumentMetadata doc : results) {
            byId.put(doc.getId().toString(), doc);
        }

        ReferenceBasicMetadata dotBasic = byId.get("short1").getReferences().get(0).getBasicMetadata();
        assertEquals("Title Only", dotBasic.getTitle().toString());
        assertEquals("Some Journal", dotBasic.getJournal().toString());
        assertNull(dotBasic.getAuthors(), "authors should not be extracted from '.'");
        assertNull(dotBasic.getYear(), "year should not be extracted from '.'");
        assertNull(dotBasic.getVolume(), "volume should not be extracted from '.'");
        assertNull(dotBasic.getSource(), "source should not be extracted from '.'");
        assertNull(byId.get("short1").getReferences().get(0).getText(),
                "'.' should not be stored as reference text");

        ReferenceBasicMetadata dotsBasic = byId.get("short2").getReferences().get(0).getBasicMetadata();
        assertEquals("Another Title", dotsBasic.getTitle().toString());
        assertNull(dotsBasic.getAuthors(), "authors should not be extracted from '..'");
        assertNull(dotsBasic.getSource(), "source should not be extracted from '..'");
        assertNull(byId.get("short2").getReferences().get(0).getText(),
                "'..' should not be stored as reference text");
    }

    @Test
    @DisplayName("Parsed documents are stored in cache and reused by a subsequent run")
    void testCacheInitializationAndReuse() throws Exception {
        // given
        Path workingDir = createTempDir("jsonRefParserTest_cache");
        Path inputDir = workingDir.resolve("input");
        Path outputDir = workingDir.resolve("output");
        Path output2Dir = workingDir.resolve("output2");
        Path outputReportDir = workingDir.resolve("output_report");
        Path outputReport2Dir = workingDir.resolve("output_report2");

        String jsonLines = ""
                + "{\"id\":\"cache1\",\"ref\":{"
                + "\"unstructured\":\"Doe, J. (2019). Cached article. Some Journal, 1(1), 1-10.\","
                + "\"article-title\":\"Cached article\","
                + "\"journal-title\":\"Some Journal\""
                + "}}\n";

        writeGzippedJson(inputDir.resolve("records.json.gz"), jsonLines);

        // when - the first run initializes the cache
        JsonReferenceParserJob.main(buildJobArgs(workingDir, inputDir, outputDir, outputReportDir));
        // and the second run over the very same input relies on the cache contents only
        JsonReferenceParserJob.main(buildJobArgs(workingDir, inputDir, output2Dir, outputReport2Dir));

        // then - both runs produce the same output
        List<ExtractedDocumentMetadata> firstRun = readDocuments(outputDir);
        List<ExtractedDocumentMetadata> secondRun = readDocuments(output2Dir);
        assertEquals(1, firstRun.size());
        assertEquals(1, secondRun.size());
        assertEquals("cache1", secondRun.get(0).getId().toString());
        assertEquals(firstRun.get(0).getReferences(), secondRun.get(0).getReferences());

        // and the parsed document is stored in the cache under the document id
        String cacheId = getExistingCacheId(workingDir);
        assertNotEquals(CacheMetadataManagingProcess.UNDEFINED, cacheId);
        List<ExtractedDocumentMetadata> cachedDocuments = readDocuments(
                CacheStorageUtils.getCacheLocation(hadoopPath(cacheRootDir(workingDir)), cacheId,
                        CacheRecordType.data).toString());
        assertEquals(1, cachedDocuments.size());
        assertEquals("cache1", cachedDocuments.get(0).getId().toString());
        assertTrue(isEmptyDataStore(CacheStorageUtils.getCacheLocation(hadoopPath(cacheRootDir(workingDir)), cacheId,
                CacheRecordType.fault).toString()));

        // and the second run reports the document as returned from the cache
        List<ReportEntry> secondRunReport = readReport(outputReport2Dir);
        assertTrue(secondRunReport.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_FROMCACHE, 1)));
        assertTrue(secondRunReport.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_PROCESSED_TOTAL, 0)));
        assertTrue(secondRunReport.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_DOCUMENTS, 1)));
    }

    @Test
    @DisplayName("Incremental run over input subset parses only the documents which are not cached yet")
    void testIncrementalRunOverInputSubset() throws Exception {
        // given - the first subset is processed and cached before the second, wider subset is run
        Path workingDir = createTempDir("jsonRefParserTest_incremental");
        Path inputDir = workingDir.resolve("input");
        Path input2Dir = workingDir.resolve("input2");
        Path outputDir = workingDir.resolve("output");
        Path output2Dir = workingDir.resolve("output2");
        Path outputReportDir = workingDir.resolve("output_report");
        Path outputReport2Dir = workingDir.resolve("output_report2");

        writeGzippedJson(inputDir.resolve("records.json.gz"),
                "{\"id\":\"inc1\",\"ref\":{\"unstructured\":\"First cached reference.\","
                        + "\"article-title\":\"First\"}}\n");
        writeGzippedJson(input2Dir.resolve("records.json.gz"),
                "{\"id\":\"inc1\",\"ref\":{\"unstructured\":\"First cached reference.\","
                        + "\"article-title\":\"First\"}}\n"
                        + "{\"id\":\"inc2\",\"ref\":{\"unstructured\":\"Second reference.\","
                        + "\"article-title\":\"Second\"}}\n");

        // when
        JsonReferenceParserJob.main(buildJobArgs(workingDir, inputDir, outputDir, outputReportDir));
        JsonReferenceParserJob.main(buildJobArgs(workingDir, input2Dir, output2Dir, outputReport2Dir));

        // then - the second run returns both the cached and the newly parsed document
        List<String> secondRunIds = new ArrayList<>();
        for (ExtractedDocumentMetadata document : readDocuments(output2Dir)) {
            secondRunIds.add(document.getId().toString());
        }
        assertTrue(secondRunIds.contains("inc1"), "cached document should be returned");
        assertTrue(secondRunIds.contains("inc2"), "newly parsed document should be returned");
        assertEquals(2, secondRunIds.size());

        // and the cache holds both documents
        String cacheId = getExistingCacheId(workingDir);
        List<ExtractedDocumentMetadata> cachedDocuments = readDocuments(
                CacheStorageUtils.getCacheLocation(hadoopPath(cacheRootDir(workingDir)), cacheId,
                        CacheRecordType.data).toString());
        assertEquals(2, cachedDocuments.size());

        // and the second run reports a single document returned from the cache
        List<ReportEntry> secondRunReport = readReport(outputReport2Dir);
        assertTrue(secondRunReport.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_FROMCACHE, 1)));
        assertTrue(secondRunReport.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_PROCESSED_TOTAL, 1)));
    }

    @Test
    @DisplayName("Fatal failure of a record is reported as a fault instead of interrupting the job")
    void testMalformedRecordReportedAsFault() throws Exception {
        // given - records whose 'ref' is not a structure at all: in the cacheless version
        // such input would interrupt the whole job
        Path workingDir = createTempDir("jsonRefParserTest_fault");
        Path inputDir = workingDir.resolve("input");
        Path outputDir = workingDir.resolve("output");
        Path output2Dir = workingDir.resolve("output2");
        Path outputReportDir = workingDir.resolve("output_report");
        Path outputReport2Dir = workingDir.resolve("output_report2");

        writeGzippedJson(inputDir.resolve("records.json.gz"),
                "{\"id\":\"fault1\",\"ref\":\"not a struct\"}\n"
                        + "{\"id\":\"fault2\",\"ref\":\"not a struct either\"}\n");

        // when - the fatal failure of a single record must not interrupt the job
        JsonReferenceParserJob.main(buildJobArgs(workingDir, inputDir, outputDir, outputReportDir));

        // then - the job completes and no document is written at the output
        assertTrue(isEmptyDataStore(outputDir.toString()), "no document should be produced");

        // and each document is reported as a fault identified with its id
        List<Fault> faults = readFaults(faultDir(outputDir));
        assertEquals(2, faults.size());
        List<String> faultIds = new ArrayList<>();
        for (Fault fault : faults) {
            faultIds.add(fault.getInputObjectId().toString());
            assertNotNull(fault.getStackTrace());
        }
        assertTrue(faultIds.contains("fault1"));
        assertTrue(faultIds.contains("fault2"));

        // and the faults are stored in the cache so the documents are not retried
        String cacheId = getExistingCacheId(workingDir);
        assertEquals(2, readFaults(CacheStorageUtils.getCacheLocation(hadoopPath(cacheRootDir(workingDir)),
                cacheId, CacheRecordType.fault).toString()).size());

        // and the counters report the faults
        List<ReportEntry> report = readReport(outputReportDir);
        assertTrue(report.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_DOCUMENTS, 0)));
        assertTrue(report.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_REFERENCES, 0)));
        assertTrue(report.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_PROCESSED_TOTAL, 2)));
        assertTrue(report.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_PROCESSED_FAULT, 2)));
        assertTrue(report.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_FROMCACHE, 0)));

        // when - the job is rerun over the very same input
        JsonReferenceParserJob.main(buildJobArgs(workingDir, inputDir, output2Dir, outputReport2Dir));

        // then - the cached faults are reused so nothing is processed and no new fault is written
        List<ReportEntry> secondRunReport = readReport(outputReport2Dir);
        assertTrue(secondRunReport.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_FROMCACHE, 2)));
        assertTrue(secondRunReport.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_PROCESSED_TOTAL, 0)));
        assertTrue(secondRunReport.contains(
                ReportEntryFactory.createCounterReportEntry(COUNTER_PROCESSED_FAULT, 0)));
        assertTrue(isEmptyDataStore(faultDir(output2Dir).toString()),
                "faults are not propagated from the cache to the output");
        assertTrue(isEmptyDataStore(output2Dir.toString()));
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    /**
     * Builds the job arguments with the mandatory cache coordinates and the fault output
     * location derived from the output location. Extra arguments may be appended at the end.
     */
    private static String[] buildJobArgs(Path workingDir, Path inputDir, Path outputDir, Path outputReportDir,
            String... extraArgs) {
        List<String> args = new ArrayList<>(Arrays.asList(
                "-sharedSparkSession",
                "-inputPath", inputDir.toString(),
                "-outputPath", outputDir.toString(),
                "-outputFaultPath", faultDir(outputDir).toString(),
                "-outputReportPath", outputReportDir.toString(),
                "-cacheRootDir", cacheRootDir(workingDir).toString(),
                "-lockManagerFactoryClassName", HadoopFsLockManagerFactory.class.getName(),
                "-numberOfEmittedFiles", "1"));
        args.addAll(Arrays.asList(extraArgs));
        return args.toArray(new String[0]);
    }

    private static Path cacheRootDir(Path workingDir) {
        return workingDir.resolve("cache");
    }

    private static Path faultDir(Path outputDir) {
        return outputDir.resolveSibling(outputDir.getFileName() + "_fault");
    }

    private static org.apache.hadoop.fs.Path hadoopPath(Path path) {
        return new org.apache.hadoop.fs.Path(path.toString());
    }

    private static String getExistingCacheId(Path workingDir) throws Exception {
        return new CacheMetadataManagingProcess().getExistingCacheId(new Configuration(),
                hadoopPath(cacheRootDir(workingDir)));
    }

    private List<ExtractedDocumentMetadata> readDocuments(Path path) {
        return readDocuments(path.toString());
    }

    private List<ExtractedDocumentMetadata> readDocuments(String path) {
        return new AvroDatasetReader(spark())
                .read(path, ExtractedDocumentMetadata.SCHEMA$, ExtractedDocumentMetadata.class)
                .collectAsList();
    }

    private List<Fault> readFaults(Path path) {
        return readFaults(path.toString());
    }

    private List<Fault> readFaults(String path) {
        return new AvroDatasetReader(spark()).read(path, Fault.SCHEMA$, Fault.class).collectAsList();
    }

    private List<ReportEntry> readReport(Path path) {
        return new AvroDatasetReader(spark()).read(path.toString(), ReportEntry.SCHEMA$, ReportEntry.class)
                .collectAsList();
    }

    /**
     * Checks whether the datastore located at the given path holds no records. Reading is not
     * done with Spark as an empty datastore cannot be turned into a Dataset.
     */
    private static boolean isEmptyDataStore(String path) throws IOException {
        return AvroTestUtils.readLocalAvroDataStore(path).isEmpty();
    }

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
