package eu.dnetlib.iis.wf.metadataextraction.crossref;

import static eu.dnetlib.iis.common.spark.SparkSessionSupport.runWithSparkSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import scala.Tuple2;

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
import eu.dnetlib.iis.wf.metadataextraction.parser.ParsedReference;
import eu.dnetlib.iis.wf.metadataextraction.parser.ParsedReferenceFiller;
import eu.dnetlib.iis.wf.metadataextraction.parser.ReferenceTextParser;
import eu.dnetlib.iis.wf.metadataextraction.parser.ReferenceTextParserFactory;
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

    private static final String DEFAULT_REFERENCE_PARSER = ReferenceTextParserFactory.PARSER_CERMINE;

    private static final int DEFAULT_GROBID_CONNECTION_TIMEOUT = 30000;

    private static final int DEFAULT_GROBID_READ_TIMEOUT = 60000;

    private static final int DEFAULT_GROBID_BATCH_SIZE = 32;

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

            String extractedBy = params.extractedBy != null ? params.extractedBy : DEFAULT_EXTRACTED_BY;
            String referenceParserType = params.referenceParser != null
                    ? params.referenceParser : DEFAULT_REFERENCE_PARSER;
            int grobidConnectionTimeout = params.grobidConnectionTimeout != null
                    ? params.grobidConnectionTimeout : DEFAULT_GROBID_CONNECTION_TIMEOUT;
            int grobidReadTimeout = params.grobidReadTimeout != null
                    ? params.grobidReadTimeout : DEFAULT_GROBID_READ_TIMEOUT;
            int grobidBatchSize = params.grobidBatchSize != null
                    ? params.grobidBatchSize : DEFAULT_GROBID_BATCH_SIZE;

            // Partition-level parse: map each input partition of (id, ref) JSON records into
            // (documentId, ReferenceMetadata) tuples. The reference parsing (CERMINE or Grobid)
            // happens here - before any shuffle - on the input partitions, fully parallel and
            // decoupled from group sizes / key skew. For Grobid, citations are sent to the server
            // in batches of grobidBatchSize per HTTP request so the number of in-flight requests
            // stays proportional to the number of running tasks (i.e. the Grobid engine capacity).
            JavaPairRDD<String, ReferenceMetadata> parsedByDocIdRdd = jsonDf.toJavaRDD()
                    .mapPartitionsToPair(new RowPartitionToReferenceMapper(referenceParserType,
                            params.grobidServerUrl, grobidConnectionTimeout, grobidReadTimeout,
                            grobidBatchSize));

            // Group only the compact ReferenceMetadata objects by document id
            JavaRDD<ExtractedDocumentMetadata> resultRdd = parsedByDocIdRdd
                    .groupByKey()
                    .map(new GroupedReferencesToDocumentMapper(extractedBy));

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

    // ----------------------------- INNER CLASSES -----------------------------

    /**
     * Spark function that maps an input partition of JSON rows (id + ref struct)
     * into (documentId, ReferenceMetadata) tuples.
     * <p>
     * The reference parsing (CERMINE or Grobid) is performed here - before any
     * shuffle - on the input partitions, fully parallel and decoupled from group
     * sizes and key skew. For the Grobid parser, citations are accumulated and
     * sent to the server in batches of {@link #batchSize} per HTTP request
     * (/api/processCitationList), keeping the number of in-flight requests
     * proportional to the number of concurrently running tasks rather than to
     * the number of rows, i.e. matched to the Grobid engine capacity.
     */
    private static class RowPartitionToReferenceMapper
            implements PairFlatMapFunction<Iterator<Row>, String, ReferenceMetadata> {

        private static final long serialVersionUID = 1L;

        private static final int DEFAULT_BATCH_SIZE = 32;

        private final String referenceParserType;

        private final String grobidServerUrl;

        private final int grobidConnectionTimeout;

        private final int grobidReadTimeout;

        private final int batchSize;

        private transient ReferenceTextParser referenceParser;

        RowPartitionToReferenceMapper(String referenceParserType, String grobidServerUrl,
                int grobidConnectionTimeout, int grobidReadTimeout, int batchSize) {
            this.referenceParserType = referenceParserType;
            this.grobidServerUrl = grobidServerUrl;
            this.grobidConnectionTimeout = grobidConnectionTimeout;
            this.grobidReadTimeout = grobidReadTimeout;
            this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
        }

        @Override
        public Iterator<Tuple2<String, ReferenceMetadata>> call(Iterator<Row> rows) throws Exception {
            List<Tuple2<String, ReferenceMetadata>> result = new ArrayList<>();
            List<PendingReference> pending = new ArrayList<>(batchSize);

            while (rows.hasNext()) {
                Row row = rows.next();
                String id = row.getString(row.fieldIndex("id"));
                if (row.isNullAt(row.fieldIndex("ref"))) {
                    continue;
                }
                Row refRow = row.getStruct(row.fieldIndex("ref"));
                String unstructured = getString(refRow, "unstructured");
                if (StringUtils.isBlank(unstructured)) {
                    // no text to parse - map only the explicitly defined JSON fields
                    ReferenceBasicMetadata basicMetadata = mapExplicitFields(refRow).build();
                    result.add(new Tuple2<>(id, buildReferenceMetadata(refRow, basicMetadata)));
                } else {
                    pending.add(new PendingReference(id, refRow, unstructured));
                    if (pending.size() >= batchSize) {
                        flushPendingBatch(pending, result);
                    }
                }
            }
            flushPendingBatch(pending, result);
            return result.iterator();
        }

        /**
         * Parses the raw text of all pending references (as a single parser batch)
         * and appends the resulting (id, ReferenceMetadata) tuples.
         */
        private void flushPendingBatch(List<PendingReference> pending,
                List<Tuple2<String, ReferenceMetadata>> result) throws Exception {
            if (pending.isEmpty()) {
                return;
            }
            try {
                List<String> texts = new ArrayList<>(pending.size());
                for (PendingReference pendingRef : pending) {
                    texts.add(pendingRef.unstructured);
                }
                List<ParsedReference> parsedList = getReferenceParser().parse(texts);
                for (int i = 0; i < pending.size(); i++) {
                    PendingReference pendingRef = pending.get(i);
                    ParsedReference parsed = parsedList != null && i < parsedList.size() ? parsedList.get(i) : null;
                    ReferenceBasicMetadata.Builder basicBuilder = mapExplicitFields(pendingRef.refRow);
                    if (parsed != null) {
                        ParsedReferenceFiller.applyParsedFields(basicBuilder, parsed);
                    }
                    result.add(new Tuple2<>(pendingRef.id,
                            buildReferenceMetadata(pendingRef.refRow, basicBuilder.build())));
                }
            } catch (Exception e) {
                // batch-level failure (e.g. one malformed citation) - fall back to
                // per-citation parsing so a single bad reference does not sink the batch
                log.warn("Batch reference parsing failed (" + pending.size()
                        + " citations); falling back to per-citation parsing", e);
                for (PendingReference pendingRef : pending) {
                    ReferenceBasicMetadata.Builder basicBuilder = mapExplicitFields(pendingRef.refRow);
                    try {
                        ParsedReference parsed = getReferenceParser().parse(pendingRef.unstructured);
                        if (parsed != null) {
                            ParsedReferenceFiller.applyParsedFields(basicBuilder, parsed);
                        }
                    } catch (Exception ex) {
                        log.warn("Unable to parse unstructured reference text: " +
                                StringUtils.abbreviate(pendingRef.unstructured, 200), ex);
                    }
                    result.add(new Tuple2<>(pendingRef.id,
                            buildReferenceMetadata(pendingRef.refRow, basicBuilder.build())));
                }
            } finally {
                pending.clear();
            }
        }

        /**
         * Returns the reference text parser, initializing it lazily on first use
         * (handles deserialization where the transient field is null).
         */
        private ReferenceTextParser getReferenceParser() {
            if (referenceParser == null) {
                referenceParser = ReferenceTextParserFactory.create(referenceParserType,
                        grobidServerUrl, grobidConnectionTimeout, grobidReadTimeout);
            }
            return referenceParser;
        }

        /**
         * Maps the explicitly defined JSON fields of a ref struct row into a
         * {@link ReferenceBasicMetadata} builder. Fields not covered here are
         * filled in later from the parsed unstructured text.
         */
        private static ReferenceBasicMetadata.Builder mapExplicitFields(Row refRow) {
            ReferenceBasicMetadata.Builder basicBuilder = ReferenceBasicMetadata.newBuilder();

            // --- Extract fields from JSON ref struct (only if non-blank) ---
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

            return basicBuilder;
        }

        /**
         * Builds a {@link ReferenceMetadata} from a ref struct row and its
         * already-populated {@link ReferenceBasicMetadata}.
         */
        private static ReferenceMetadata buildReferenceMetadata(Row refRow, ReferenceBasicMetadata basicMetadata) {
            ReferenceMetadata.Builder refBuilder = ReferenceMetadata.newBuilder();

            // --- Build ReferenceMetadata ---
            refBuilder.setBasicMetadata(basicMetadata);

            // ref#unstructured -> references[]#text
            String unstructured = getString(refRow, "unstructured");
            if (StringUtils.isNotBlank(unstructured)) {
                refBuilder.setText(unstructured);
            }

            // position is explicitly left unset (unknown ordering)
            return refBuilder.build();
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

    /**
     * Reference awaiting parsing - keeps the id, the raw ref row and the
     * unstructured text together so a parsed result can be mapped back by index.
     */
    private static class PendingReference {

        private final String id;

        private final Row refRow;

        private final String unstructured;

        PendingReference(String id, Row refRow, String unstructured) {
            this.id = id;
            this.refRow = refRow;
            this.unstructured = unstructured;
        }
    }

    // ----------------------------- INNER TYPES -----------------------------

    /**
     * Spark function that maps a grouped (documentId, [references...]) tuple to
     * an {@link ExtractedDocumentMetadata} Avro record.
     */
    private static class GroupedReferencesToDocumentMapper
            implements Function<Tuple2<String, Iterable<ReferenceMetadata>>, ExtractedDocumentMetadata> {

        private static final long serialVersionUID = 1L;

        private final String extractedBy;

        GroupedReferencesToDocumentMapper(String extractedBy) {
            this.extractedBy = extractedBy;
        }

        @Override
        public ExtractedDocumentMetadata call(Tuple2<String, Iterable<ReferenceMetadata>> t) throws Exception {
            String id = t._1();
            Iterable<ReferenceMetadata> refs = t._2();

            List<ReferenceMetadata> referenceMetadatas = new ArrayList<>();
            if (refs != null) {
                for (ReferenceMetadata ref : refs) {
                    referenceMetadatas.add(ref);
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

        @Parameter(names = "-referenceParser", required = false,
                description = "reference text parser to use: 'cermine' (default) or 'grobid'")
        private String referenceParser;

        @Parameter(names = "-grobidServerUrl", required = false,
                description = "Grobid server location, required when -referenceParser is set to 'grobid'")
        private String grobidServerUrl;

        @Parameter(names = "-grobidConnectionTimeout", required = false,
                description = "Grobid connection timeout in ms")
        private Integer grobidConnectionTimeout;

        @Parameter(names = "-grobidReadTimeout", required = false,
                description = "Grobid read timeout in ms")
        private Integer grobidReadTimeout;

        @Parameter(names = "-grobidBatchSize", required = false,
                description = "number of citations sent to Grobid per /api/processCitationList request (default 32)")
        private Integer grobidBatchSize;
    }
}
