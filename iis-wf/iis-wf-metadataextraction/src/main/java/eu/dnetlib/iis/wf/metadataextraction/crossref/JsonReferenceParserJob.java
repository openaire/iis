package eu.dnetlib.iis.wf.metadataextraction.crossref;

import static eu.dnetlib.iis.common.spark.SparkSessionSupport.runWithSparkSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.Range;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata;
import pl.edu.icm.cermine.bibref.CRFBibReferenceParser;
import pl.edu.icm.cermine.bibref.model.BibEntry;
import pl.edu.icm.cermine.bibref.model.BibEntryFieldType;
import pl.edu.icm.cermine.exception.AnalysisException;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Job responsible for parsing JSON-formatted bibliographic references and converting them
 * into {@link ExtractedDocumentMetadata} Avro records.
 * <p>
 * Input consists of gzip-compressed JSON packages, each containing one JSON record per line.
 * Records are grouped by the {@code id} field. Fields explicitly defined in the JSON record
 * are mapped directly to the target Avro model; the raw unstructured reference text is parsed
 * using CERMINE's {@link CRFBibReferenceParser} to populate fields not covered by the explicit mapping.
 *
 * @author mhorst
 */
public class JsonReferenceParserJob {

    private static final Logger log = Logger.getLogger(JsonReferenceParserJob.class);

    private static SparkAvroSaver avroSaver = new SparkAvroSaver();

    private static final String DEFAULT_EXTRACTED_BY = "crossrefBibrefParser";

    private static final String COUNTER_PROCESSED_DOCUMENTS = "processing.crossref.referenceParser.documents";

    private static final String COUNTER_PROCESSED_REFERENCES = "processing.crossref.referenceParser.references";

    // ----------------------------- MAIN -----------------------------

    public static void main(String[] args) throws Exception {
        JsonReferenceParserJobParameters params = new JsonReferenceParserJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        SparkConf conf = new SparkConf();
        runWithSparkSession(conf, params.isSparkSessionShared, spark -> {

            HdfsUtils.remove(spark.sparkContext().hadoopConfiguration(), params.outputPath);
            HdfsUtils.remove(spark.sparkContext().hadoopConfiguration(), params.outputReportPath);

            // Read JSON input (gzip compressed, one JSON record per line)
            Dataset<Row> jsonDf = spark.read().json(params.inputPath);

            // Group by id and collect all ref structs into a list
            Dataset<Row> groupedDf = jsonDf.groupBy("id")
                    .agg(functions.collect_list("ref").as("refs"));

            // Transform grouped rows into ExtractedDocumentMetadata Avro records
            String extractedBy = params.extractedBy != null ? params.extractedBy : DEFAULT_EXTRACTED_BY;
            CRFBibReferenceParser referenceParser = initReferenceParser();

            JavaRDD<ExtractedDocumentMetadata> resultRdd = groupedDf.toJavaRDD()
                    .map(new GroupedRowToDocumentMapper(extractedBy, referenceParser));

            resultRdd.cache();

            long documentCount = resultRdd.count();
            long referenceCount = resultRdd
                    .map(doc -> (long) (doc.getReferences() != null ? doc.getReferences().size() : 0))
                    .reduce(Long::sum);

            JavaRDD<ReportEntry> reportRdd = spark.createDataset(
                    java.util.Arrays.asList(
                            ReportEntryFactory.createCounterReportEntry(COUNTER_PROCESSED_DOCUMENTS, documentCount),
                            ReportEntryFactory.createCounterReportEntry(COUNTER_PROCESSED_REFERENCES, referenceCount)),
                    org.apache.spark.sql.Encoders.kryo(ReportEntry.class)).javaRDD();

            avroSaver.saveJavaRDD(resultRdd, ExtractedDocumentMetadata.SCHEMA$, params.outputPath);
            avroSaver.saveJavaRDD(reportRdd, ReportEntry.SCHEMA$, params.outputReportPath);
        });
    }

    // ----------------------------- PRIVATE -----------------------------

    /**
     * Initializes the CERMINE CRF-based reference parser.
     */
    private static CRFBibReferenceParser initReferenceParser() {
        try {
            return CRFBibReferenceParser.getInstance();
        } catch (AnalysisException e) {
            throw new RuntimeException("Unable to initialize CRF BibReference parser", e);
        }
    }

    // ----------------------------- INNER CLASSES -----------------------------

    /**
     * Spark function that maps a grouped row (id, [ref1, ref2, ...]) to
     * an {@link ExtractedDocumentMetadata} Avro record.
     */
    private static class GroupedRowToDocumentMapper
            implements Function<Row, ExtractedDocumentMetadata> {

        private static final long serialVersionUID = 1L;

        private final String extractedBy;

        private final transient CRFBibReferenceParser referenceParser;

        GroupedRowToDocumentMapper(String extractedBy, CRFBibReferenceParser referenceParser) {
            this.extractedBy = extractedBy;
            this.referenceParser = referenceParser;
        }

        @Override
        public ExtractedDocumentMetadata call(Row row) throws Exception {
            String id = row.getString(row.fieldIndex("id"));
            List<Row> refs = row.getList(row.fieldIndex("refs"));

            List<ReferenceMetadata> referenceMetadatas = new ArrayList<>();
            if (refs != null) {
                for (Row refRow : refs) {
                    ReferenceMetadata refMeta = buildReferenceMetadata(refRow);
                    referenceMetadatas.add(refMeta);
                }
            }

            ExtractedDocumentMetadata.Builder builder = ExtractedDocumentMetadata.newBuilder();
            builder.setId(id);
            builder.setExtractedBy(extractedBy);
            builder.setText("");

            if (!referenceMetadatas.isEmpty()) {
                builder.setReferences(referenceMetadatas);
            }

            return builder.build();
        }

        /**
         * Returns the reference parser, re-initializing it if needed
         * (handles deserialization where the transient field is null).
         */
        private CRFBibReferenceParser getReferenceParser() {
            if (referenceParser != null) {
                return referenceParser;
            }
            return initReferenceParser();
        }

        /**
         * Builds a {@link ReferenceMetadata} from a single JSON ref struct row.
         * Directly mapped fields take precedence; the raw unstructured text is
         * parsed with CERMINE to fill in remaining fields.
         */
        private ReferenceMetadata buildReferenceMetadata(Row refRow) {
            ReferenceBasicMetadata.Builder basicBuilder = ReferenceBasicMetadata.newBuilder();
            ReferenceMetadata.Builder refBuilder = ReferenceMetadata.newBuilder();

            // --- Extract fields from JSON ref struct (only if non-blank) ---
            String unstructured = getString(refRow, "unstructured");
            String articleTitle = getString(refRow, "article-title");
            String edition = getString(refRow, "edition");
            String firstPage = getString(refRow, "first-page");
            String issue = getString(refRow, "issue");
            String journalTitle = getString(refRow, "journal-title");
            String volume = getString(refRow, "volume");
            String year = getString(refRow, "year");
            String seriesTitle = getString(refRow, "series-title");
            String type = getString(refRow, "type");
            String doi = getString(refRow, "DOI");
            String issn = getString(refRow, "ISSN");
            String isbn = getString(refRow, "ISBN");

            // --- Map directly from JSON fields ---

            // ref#article-title -> references[]#basicMetadata#title
            if (StringUtils.isNotBlank(articleTitle)) {
                basicBuilder.setTitle(articleTitle);
            }

            // ref#edition -> references[]#basicMetadata#edition
            if (StringUtils.isNotBlank(edition)) {
                basicBuilder.setEdition(edition);
            }

            // ref#first-page -> references[]#basicMetadata#pages#start
            if (StringUtils.isNotBlank(firstPage)) {
                basicBuilder.setPages(Range.newBuilder().setStart(firstPage).build());
            }

            // ref#issue -> references[]#basicMetadata#issue
            if (StringUtils.isNotBlank(issue)) {
                basicBuilder.setIssue(issue);
            }

            // ref#journal-title -> references[]#basicMetadata#journal
            if (StringUtils.isNotBlank(journalTitle)) {
                basicBuilder.setJournal(journalTitle);
            }

            // ref#volume -> references[]#basicMetadata#volume
            if (StringUtils.isNotBlank(volume)) {
                basicBuilder.setVolume(volume);
            }

            // ref#year -> references[]#basicMetadata#year
            if (StringUtils.isNotBlank(year)) {
                basicBuilder.setYear(year);
            }

            // ref#series-title -> references[]#basicMetadata#series
            if (StringUtils.isNotBlank(seriesTitle)) {
                basicBuilder.setSeries(seriesTitle);
            }

            // ref#type -> references[]#basicMetadata#type
            // If ref#type was not explicitly defined and ref#ISBN was set to a non blank value
            // we should set references[]#basicMetadata#type to a 'book' value.
            if (StringUtils.isNotBlank(type)) {
                basicBuilder.setType(type);
            } else if (StringUtils.isNotBlank(isbn)) {
                basicBuilder.setType("book");
            }

            // ref#DOI -> references[]#basicMetadata#externalIds['doi']
            // ref#ISSN -> references[]#basicMetadata#externalIds['ISSN']
            // ref#ISBN -> references[]#basicMetadata#externalIds['ISBN']
            Map<CharSequence, CharSequence> externalIds = new HashMap<>();
            if (StringUtils.isNotBlank(doi)) {
                externalIds.put("doi", doi);
            }
            if (StringUtils.isNotBlank(issn)) {
                externalIds.put("ISSN", issn);
            }
            if (StringUtils.isNotBlank(isbn)) {
                externalIds.put("ISBN", isbn);
            }
            if (!externalIds.isEmpty()) {
                basicBuilder.setExternalIds(externalIds);
            }

            // --- Parse unstructured text with CERMINE to fill remaining fields ---
            if (StringUtils.isNotBlank(unstructured)) {
                try {
                    BibEntry bibEntry = getReferenceParser().parseBibReference(unstructured);
                    if (bibEntry != null) {
                        applyParsedFields(basicBuilder, bibEntry);
                    }
                } catch (AnalysisException e) {
                    log.warn("Unable to parse unstructured reference text: " +
                            StringUtils.abbreviate(unstructured, 200), e);
                }
            }

            // --- Build ReferenceMetadata ---
            refBuilder.setBasicMetadata(basicBuilder.build());

            // ref#unstructured -> references[]#text
            if (StringUtils.isNotBlank(unstructured)) {
                refBuilder.setText(unstructured);
            }

            // position is explicitly left unset (unknown ordering)
            return refBuilder.build();
        }

        /**
         * Applies fields parsed from the raw unstructured text via CERMINE,
         * but only for fields that have NOT already been set from the JSON mapping.
         */
        private void applyParsedFields(ReferenceBasicMetadata.Builder basicBuilder, BibEntry bibEntry) {

            // title - only if not already set from ref#article-title
            if (basicBuilder.getTitle() == null) {
                String parsedTitle = bibEntry.getFirstFieldValue(BibEntryFieldType.TITLE);
                if (StringUtils.isNotBlank(parsedTitle)) {
                    basicBuilder.setTitle(parsedTitle);
                }
            }

            // authors - never explicitly set from JSON mapping
            List<CharSequence> authors = new ArrayList<>();
            List<String> parsedAuthors = bibEntry.getAllFieldValues(BibEntryFieldType.AUTHOR);
            if (parsedAuthors != null) {
                for (String author : parsedAuthors) {
                    if (StringUtils.isNotBlank(author)) {
                        authors.add(author);
                    }
                }
            }
            if (!authors.isEmpty()) {
                basicBuilder.setAuthors(authors);
            }

            // pages - only if not already set from ref#first-page
            if (basicBuilder.getPages() == null) {
                // FIXME make sure this is the way to get pages! Check the CERMINE code in metadataextraction
                // this needs to be fixed, rely on NlmToDocumentWithBasicMetadataConverter#convertBibEntry()
                // check also the way other fields are being extracted!
                String parsedPages = bibEntry.getFirstFieldValue(BibEntryFieldType.PAGES);
                if (StringUtils.isNotBlank(parsedPages)) {
                    Range pagesRange = parsePagesRange(parsedPages);
                    if (pagesRange != null) {
                        basicBuilder.setPages(pagesRange);
                    }
                }
            }

            // journal - only if not already set from ref#journal-title
            if (basicBuilder.getJournal() == null) {
                String parsedJournal = bibEntry.getFirstFieldValue(BibEntryFieldType.JOURNAL);
                if (StringUtils.isNotBlank(parsedJournal)) {
                    basicBuilder.setJournal(parsedJournal);
                }
            }

            // source - never explicitly set from JSON mapping
            String parsedSource = bibEntry.getFirstFieldValue(BibEntryFieldType.JOURNAL);
            if (StringUtils.isBlank(parsedSource)) {
                parsedSource = bibEntry.getFirstFieldValue(BibEntryFieldType.TITLE);
            }
            if (StringUtils.isNotBlank(parsedSource) && basicBuilder.getSource() == null) {
                basicBuilder.setSource(parsedSource);
            }

            // volume - only if not already set from ref#volume
            if (basicBuilder.getVolume() == null) {
                String parsedVolume = bibEntry.getFirstFieldValue(BibEntryFieldType.VOLUME);
                if (StringUtils.isNotBlank(parsedVolume)) {
                    basicBuilder.setVolume(parsedVolume);
                }
            }

            // year - only if not already set from ref#year
            if (basicBuilder.getYear() == null) {
                String parsedYear = bibEntry.getFirstFieldValue(BibEntryFieldType.YEAR);
                if (StringUtils.isNotBlank(parsedYear)) {
                    basicBuilder.setYear(parsedYear);
                }
            }

            // edition - only if not already set from ref#edition
            if (basicBuilder.getEdition() == null) {
                String parsedEdition = bibEntry.getFirstFieldValue(BibEntryFieldType.EDITION);
                if (StringUtils.isNotBlank(parsedEdition)) {
                    basicBuilder.setEdition(parsedEdition);
                }
            }

            // publisher - never explicitly set from JSON mapping
            String parsedPublisher = bibEntry.getFirstFieldValue(BibEntryFieldType.PUBLISHER);
            if (StringUtils.isNotBlank(parsedPublisher)) {
                basicBuilder.setPublisher(parsedPublisher);
            }

            // location - never explicitly set from JSON mapping
            String parsedLocation = bibEntry.getFirstFieldValue(BibEntryFieldType.LOCATION);
            if (StringUtils.isNotBlank(parsedLocation)) {
                basicBuilder.setLocation(parsedLocation);
            }

            // series - only if not already set from ref#series-title
            if (basicBuilder.getSeries() == null) {
                String parsedSeries = bibEntry.getFirstFieldValue(BibEntryFieldType.SERIES);
                if (StringUtils.isNotBlank(parsedSeries)) {
                    basicBuilder.setSeries(parsedSeries);
                }
            }

            // issue - only if not already set from ref#issue
            if (basicBuilder.getIssue() == null) {
                String parsedIssue = bibEntry.getFirstFieldValue(BibEntryFieldType.NUMBER);
                if (StringUtils.isNotBlank(parsedIssue)) {
                    basicBuilder.setIssue(parsedIssue);
                }
            }

            // url - never explicitly set from JSON mapping
            String parsedUrl = bibEntry.getFirstFieldValue(BibEntryFieldType.URL);
            if (StringUtils.isNotBlank(parsedUrl)) {
                basicBuilder.setUrl(parsedUrl);
            }

            // externalIds - only fill in identifiers not already mapped from JSON
            Map<CharSequence, CharSequence> existingExtIds = basicBuilder.getExternalIds();
            if (existingExtIds == null) {
                existingExtIds = new HashMap<>();
            }

            String parsedDoi = bibEntry.getFirstFieldValue(BibEntryFieldType.DOI);
            if (StringUtils.isNotBlank(parsedDoi) && !existingExtIds.containsKey("doi")) {
                existingExtIds.put("doi", parsedDoi);
            }

            String parsedIsbn = bibEntry.getFirstFieldValue(BibEntryFieldType.ISBN);
            if (StringUtils.isNotBlank(parsedIsbn) && !existingExtIds.containsKey("ISBN")) {
                existingExtIds.put("ISBN", parsedIsbn);
            }

            String parsedIssn = bibEntry.getFirstFieldValue(BibEntryFieldType.ISSN);
            if (StringUtils.isNotBlank(parsedIssn) && !existingExtIds.containsKey("ISSN")) {
                existingExtIds.put("ISSN", parsedIssn);
            }

            if (!existingExtIds.isEmpty()) {
                basicBuilder.setExternalIds(existingExtIds);
            }
        }

        /**
         * Parses a page range string (e.g. "149-187" or "149") into a {@link Range} object.
         */
        private static Range parsePagesRange(String pagesStr) {
            if (StringUtils.isBlank(pagesStr)) {
                return null;
            }
            String trimmed = pagesStr.trim();
            String[] parts = trimmed.split("[-–—]+");
            if (parts.length == 1) {
                String single = parts[0].trim();
                if (StringUtils.isNotBlank(single)) {
                    return Range.newBuilder().setStart(single).build();
                }
            } else if (parts.length >= 2) {
                String start = parts[0].trim();
                String end = parts[parts.length - 1].trim();
                if (StringUtils.isNotBlank(start)) {
                    Range.Builder rangeBuilder = Range.newBuilder().setStart(start);
                    if (StringUtils.isNotBlank(end)) {
                        rangeBuilder.setEnd(end);
                    }
                    return rangeBuilder.build();
                }
            }
            return null;
        }

        /**
         * Safely extracts a string value from a Row, returning null if the field
         * is missing or contains a SQL null.
         */
        private static String getString(Row row, String fieldName) {
            try {
                int idx = row.fieldIndex(fieldName);
                if (row.isNullAt(idx)) {
                    return null;
                }
                Object val = row.get(idx);
                return val != null ? val.toString() : null;
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    // ----------------------------- PARAMETERS -----------------------------

    @Parameters(separators = "=")
    private static class JsonReferenceParserJobParameters {

        @Parameter(names = "-sharedSparkSession")
        private Boolean isSparkSessionShared = Boolean.FALSE;

        @Parameter(names = "-inputPath", required = true,
                description = "path to the input JSON datastore (gzip compressed packages, one JSON record per line)")
        private String inputPath;

        @Parameter(names = "-outputPath", required = true,
                description = "path to the output Avro datastore with ExtractedDocumentMetadata records")
        private String outputPath;

        @Parameter(names = "-outputReportPath", required = true,
                description = "path to the output report")
        private String outputReportPath;

        @Parameter(names = "-extractedBy", required = false,
                description = "value to set in ExtractedDocumentMetadata#extractedBy field")
        private String extractedBy;
    }
}
