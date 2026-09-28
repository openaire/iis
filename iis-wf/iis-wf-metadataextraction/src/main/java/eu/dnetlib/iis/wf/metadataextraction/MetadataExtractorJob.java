package eu.dnetlib.iis.wf.metadataextraction;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.jdom.Document;
import org.jdom.Element;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;
import com.itextpdf.text.exceptions.InvalidPdfException;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.fault.FaultUtils;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.importer.schemas.DocumentContent;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.wf.importer.content.approver.ContentApprover;
import eu.dnetlib.iis.wf.importer.content.approver.PDFHeaderBasedContentApprover;
import eu.dnetlib.iis.wf.metadataextraction.grobid.GrobidClient;
import eu.dnetlib.iis.wf.metadataextraction.grobid.TeiToExtractedDocumentMetadataTransformer;
import pl.edu.icm.cermine.ContentExtractor;
import pl.edu.icm.cermine.configuration.ExtractionConfigBuilder;
import pl.edu.icm.cermine.configuration.ExtractionConfigProperty;
import pl.edu.icm.cermine.configuration.ExtractionConfigRegister;
import pl.edu.icm.cermine.exception.AnalysisException;
import pl.edu.icm.cermine.exception.TransformationException;
import pl.edu.icm.cermine.tools.timeout.TimeoutException;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Spark port of {@link MetadataExtractorMapper}.
 *
 * <p>Performs PDF metadata extraction for {@link DocumentContent} records producing three
 * named outputs:</p>
 * <ul>
 *   <li>{@code meta} - {@link ExtractedDocumentMetadata} records (either successfully extracted
 *       or empty marker records written for failed documents)</li>
 *   <li>{@code fault} - {@link Fault} records for documents that could not be processed
 *       (persisted in cache so they are not retried)</li>
 *   <li>{@code transientfault} - {@link Fault} records for provisional failures which are NOT
 *       persisted in cache (for analysis only)</li>
 * </ul>
 *
 * <p>The extraction is realized either by an external Grobid server (when
 * {@code -grobidServerUrl} is defined) or by the embedded CERMINE library.</p>
 *
 * <p>The logic of {@link #extract(JavaSparkContext, JavaRDD, Params)} is reused in-process by
 * {@link eu.dnetlib.iis.wf.metadataextraction.cache.MetadataExtractionCacheChainJob} so that the
 * cache management wrapper (ported from the Oozie cache chain/create/update workflows) does not
 * have to re-implement the extraction itself.</p>
 */
public class MetadataExtractorJob {

    private static final Logger log = Logger.getLogger(MetadataExtractorJob.class);

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    public static final String UNDEFINED = "$UNDEFINED$";

    public static final String EMPTY_META = "$EMPTY$";

    public static final String EXTRACTED_METADATA_RECORD_ORIGIN_CERMINE = "CERMINE";
    public static final String EXTRACTED_METADATA_RECORD_ORIGIN_GROBID = "GROBID";
    public static final String EXTRACTED_METADATA_RECORD_ORIGIN_UNSPECIFIED = "UNSPECIFIED";

    public static final String FAULT_CODE_PROCESSING_TIME_THRESHOLD_EXCEEDED = "ProcessingTimeThresholdExceeded";
    public static final String FAULT_SUPPLEMENTARY_DATA_PROCESSING_TIME = "processing_time";

    private static final String INVALID_PDF_HEADER_MSG = "content PDF header not approved!";

    // Report entry keys (mirror the Oozie ReportGenerator properties defined in the
    // eu/dnetlib/iis/wf/metadataextraction/core/oozie_app workflow).
    public static final String REPORT_KEY_PROCESSED_DOC_METADATA = "import.metadataExtraction.processed.docMetadata";
    public static final String REPORT_KEY_PROCESSED_FAULT_TOTAL = "import.metadataExtraction.processed.fault.total";
    public static final String REPORT_KEY_PROCESSED_TRANSIENT_ERROR = "import.metadataExtraction.processed.transientError";
    public static final String REPORT_KEY_PROCESSED_FAULT_INVALID_PDF = "import.metadataExtraction.processed.fault.invalidPdf";

    private static final long SECS_TO_MILLIS = 1000L;

    private MetadataExtractorJob() {
    }

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws Exception {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputRoot);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);

            JavaRDD<DocumentContent> input = avroLoader.loadJavaRDD(sc, params.input, DocumentContent.class);

            ExtractionResult result = extract(sc, input, params);

            avroSaver.saveJavaRDD(result.meta, ExtractedDocumentMetadata.SCHEMA$,
                    params.outputRoot + "/" + params.outputNameMeta);
            avroSaver.saveJavaRDD(result.fault, Fault.SCHEMA$,
                    params.outputRoot + "/" + params.outputNameFault);
            avroSaver.saveJavaRDD(result.transientFault, Fault.SCHEMA$,
                    params.outputRoot + "/" + params.outputNameTransientFault);
            if (StringUtils.isNotBlank(params.outputReportPath)) {
                avroSaver.saveJavaRDD(result.reports.repartition(1), ReportEntry.SCHEMA$,
                        params.outputReportPath);
            }
        }
    }

    /**
     * Runs the metadata extraction over the given input returning the three output datastores plus
     * the report entries describing the processing result. The returned RDDs are cached, so the
     * caller may safely derive several outputs without recomputing the extraction.
     */
    public static ExtractionResult extract(JavaSparkContext sc, JavaRDD<DocumentContent> input, Params params) {
        LongAccumulator invalidPdfCounter = sc.sc().longAccumulator("INVALID_PDF_HEADER");
        Set<String> excludedIds = params.excludedIds();

        JavaRDD<ExtractionRecord> records = input
                .filter(content -> !excludedIds.contains(content.getId().toString()))
                .mapPartitions(partition -> new ExtractionPartitionIterator(partition, params, invalidPdfCounter))
                .filter(record -> record != null);
        records.cache();

        JavaRDD<ExtractedDocumentMetadata> meta = records
                .filter(record -> record.meta != null)
                .map(record -> record.meta);
        JavaRDD<Fault> fault = records
                .filter(record -> record.fault != null)
                .map(record -> record.fault);
        JavaRDD<Fault> transientFault = records
                .filter(record -> record.transientFault != null)
                .map(record -> record.transientFault);

        long metaCount = meta.count();
        long faultCount = fault.count();
        long transientFaultCount = transientFault.count();

        JavaRDD<ReportEntry> reports = sc.parallelize(Lists.newArrayList(
                ReportEntryFactory.createCounterReportEntry(REPORT_KEY_PROCESSED_DOC_METADATA, metaCount),
                ReportEntryFactory.createCounterReportEntry(REPORT_KEY_PROCESSED_FAULT_TOTAL, faultCount),
                ReportEntryFactory.createCounterReportEntry(REPORT_KEY_PROCESSED_TRANSIENT_ERROR, transientFaultCount),
                ReportEntryFactory.createCounterReportEntry(REPORT_KEY_PROCESSED_FAULT_INVALID_PDF,
                        invalidPdfCounter.value())));

        return new ExtractionResult(meta, fault, transientFault, reports, metaCount, faultCount,
                transientFaultCount);
    }

    /**
     * Builds a "nothing was processed" extraction result (all counters zero) used when the
     * extraction phase is deliberately skipped (e.g. metadata extraction disabled in the cache
     * chain or empty input).
     */
    public static ExtractionResult emptyResult(JavaSparkContext sc) {
        JavaRDD<ReportEntry> reports = sc.parallelize(Lists.newArrayList(
                ReportEntryFactory.createCounterReportEntry(REPORT_KEY_PROCESSED_DOC_METADATA, 0L),
                ReportEntryFactory.createCounterReportEntry(REPORT_KEY_PROCESSED_FAULT_TOTAL, 0L),
                ReportEntryFactory.createCounterReportEntry(REPORT_KEY_PROCESSED_TRANSIENT_ERROR, 0L),
                ReportEntryFactory.createCounterReportEntry(REPORT_KEY_PROCESSED_FAULT_INVALID_PDF, 0L)));
        return new ExtractionResult(sc.emptyRDD(), sc.emptyRDD(), sc.emptyRDD(), reports, 0L, 0L, 0L);
    }

    /**
     * Creates empty metadata entry with identifier set and empty record indicator.
     * Never returns null.
     */
    public static ExtractedDocumentMetadata createEmpty(String id, String extractedBy) {
        if (id == null) {
            throw new IllegalArgumentException("unable to set null id");
        }
        return ExtractedDocumentMetadata.newBuilder()
                .setId(id)
                .setText("")
                .setPublicationTypeName(EMPTY_META)
                .setExtractedBy(extractedBy)
                .build();
    }

    //------------------------ PRIVATE --------------------------

    private static byte[] toByteArray(ByteBuffer byteBuffer) {
        ByteBuffer duplicate = byteBuffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    /**
     * Result holder for a single input record. Exactly one of the following layouts holds:
     * <ul>
     *   <li>{@code meta} only - successful extraction (optionally accompanied by a
     *       processing-time {@code fault})</li>
     *   <li>{@code meta} + {@code fault} - failed extraction: empty marker record + fault</li>
     *   <li>{@code transientFault} only - provisional failure, not persisted in cache</li>
     * </ul>
     */
    static class ExtractionRecord implements Serializable {

        private static final long serialVersionUID = 1L;

        private final ExtractedDocumentMetadata meta;
        private final Fault fault;
        private final Fault transientFault;

        private ExtractionRecord(ExtractedDocumentMetadata meta, Fault fault, Fault transientFault) {
            this.meta = meta;
            this.fault = fault;
            this.transientFault = transientFault;
        }

        static ExtractionRecord ofMeta(ExtractedDocumentMetadata meta, Fault fault) {
            return new ExtractionRecord(meta, fault, null);
        }

        static ExtractionRecord ofTransientFault(Fault transientFault) {
            return new ExtractionRecord(null, null, transientFault);
        }
    }

    /**
     * Carries the outcome of {@link #extract(JavaSparkContext, JavaRDD, Params)}.
     */
    public static class ExtractionResult implements Serializable {

        private static final long serialVersionUID = 1L;

        public final JavaRDD<ExtractedDocumentMetadata> meta;
        public final JavaRDD<Fault> fault;
        public final JavaRDD<Fault> transientFault;
        public final JavaRDD<ReportEntry> reports;
        public final long metaCount;
        public final long faultCount;
        public final long transientFaultCount;

        ExtractionResult(JavaRDD<ExtractedDocumentMetadata> meta, JavaRDD<Fault> fault,
                JavaRDD<Fault> transientFault, JavaRDD<ReportEntry> reports,
                long metaCount, long faultCount, long transientFaultCount) {
            this.meta = meta;
            this.fault = fault;
            this.transientFault = transientFault;
            this.reports = reports;
            this.metaCount = metaCount;
            this.faultCount = faultCount;
            this.transientFaultCount = transientFaultCount;
        }
    }

    /**
     * Per-partition iterator performing the actual extraction, holding the (non-serializable)
     * Grobid client / CERMINE configuration for the lifetime of a single partition.
     */
    static class ExtractionPartitionIterator implements Iterator<ExtractionRecord> {

        private final Iterator<DocumentContent> input;
        private final Params params;
        private final LongAccumulator invalidPdfCounter;

        private final ContentApprover contentApprover = new PDFHeaderBasedContentApprover();
        private final GrobidClient grobidClient;
        private final String grobidServerVersion;
        private final long processingTimeThreshold;

        private ExtractionRecord nextRecord;
        private boolean exhausted;
        private int currentProgress = 0;
        private long intervalTime;

        ExtractionPartitionIterator(Iterator<DocumentContent> input, Params params,
                LongAccumulator invalidPdfCounter) {
            this.input = input;
            this.params = params;
            this.invalidPdfCounter = invalidPdfCounter;

            if (StringUtils.isNotBlank(params.grobidServerUrl) && !UNDEFINED.equals(params.grobidServerUrl)) {
                log.info("enabling metadata extraction relying on Grobid, url address: " + params.grobidServerUrl);
                this.grobidClient = new GrobidClient(params.grobidServerUrl, params.grobidServerConnectionTimeout,
                        params.grobidServerReadTimeout, params.grobidServerThrottleSleepTime,
                        params.grobidServerMaxRetriesCount);
                this.grobidServerVersion = StringUtils.isNotBlank(params.grobidServerVersion)
                        ? params.grobidServerVersion.trim()
                        : EXTRACTED_METADATA_RECORD_ORIGIN_GROBID;
            } else {
                log.info("enabling metadata extraction relying on CERMINE");
                this.grobidClient = null;
                this.grobidServerVersion = null;
            }
            this.processingTimeThreshold = params.processingTimeThresholdMillis();
            this.intervalTime = System.currentTimeMillis();
        }

        @Override
        public boolean hasNext() {
            if (nextRecord == null && !exhausted) {
                nextRecord = fetchNext();
            }
            return nextRecord != null;
        }

        @Override
        public ExtractionRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            ExtractionRecord result = nextRecord;
            nextRecord = null;
            return result;
        }

        private ExtractionRecord fetchNext() {
            while (input.hasNext()) {
                try {
                    ExtractionRecord record = process(input.next());
                    if (record != null) {
                        return record;
                    }
                } catch (Exception e) {
                    // never fail the whole partition on a single unexpected record
                    log.error("unexpected error while processing record, skipping", e);
                }
            }
            close();
            return null;
        }

        private ExtractionRecord process(DocumentContent content) {
            String documentId = content.getId().toString();

            if (content.getPdf() == null) {
                log.warn("no byte data found for id: " + content.getId());
                return null;
            }

            byte[] bytes = toByteArray(content.getPdf());

            if (!contentApprover.approve(bytes)) {
                log.info(INVALID_PDF_HEADER_MSG);
                invalidPdfCounter.add(1);
                return handleException(new InvalidPdfException(INVALID_PDF_HEADER_MSG), documentId,
                        EXTRACTED_METADATA_RECORD_ORIGIN_UNSPECIFIED, false);
            }

            logProgress();

            long startTime = System.currentTimeMillis();
            String extractedBy = EXTRACTED_METADATA_RECORD_ORIGIN_UNSPECIFIED;
            try {
                if (grobidClient != null) {
                    extractedBy = grobidServerVersion;
                    String teiXml = grobidClient.processPdfByteBuffer(ByteBuffer.wrap(bytes));
                    ExtractedDocumentMetadata meta = TeiToExtractedDocumentMetadataTransformer
                            .transformToExtractedDocumentMetadata(documentId, teiXml, grobidServerVersion);
                    return handleSuccess(meta, startTime, documentId);
                }
                extractedBy = EXTRACTED_METADATA_RECORD_ORIGIN_CERMINE;
                ExtractedDocumentMetadata meta = processWithCermine(documentId, bytes);
                return handleSuccess(meta, startTime, documentId);
            } catch (TransientException e) {
                log.error("Provisional exception occurred while handling document! "
                        + "This means fault is not going to be written in output, error is logged only in order "
                        + "to allow further processing!", e);
                handleProcessingTime(System.currentTimeMillis() - startTime, documentId, false);
                return ExtractionRecord.ofTransientFault(FaultUtils.exceptionToFault(documentId, e, null));
            } catch (Exception e) {
                handleProcessingTime(System.currentTimeMillis() - startTime, documentId, false);
                return handleException(e, documentId, extractedBy, true);
            }
        }

        private ExtractionRecord handleSuccess(ExtractedDocumentMetadata meta, long startTime, String documentId) {
            Fault processingTimeFault = buildProcessingTimeFault(System.currentTimeMillis() - startTime, documentId);
            logFinished(documentId, System.currentTimeMillis() - startTime);
            return ExtractionRecord.ofMeta(meta, processingTimeFault);
        }

        private ExtractionRecord handleException(Exception e, String documentId, String extractedBy,
                boolean writeMeta) {
            Fault fault = FaultUtils.exceptionToFault(documentId, e, null);
            if (writeMeta) {
                // writing empty result alongside the fault
                return ExtractionRecord.ofMeta(createEmpty(documentId, extractedBy), fault);
            }
            // no metadata was produced for this record (e.g. invalid PDF header)
            return ExtractionRecord.ofMeta(null, fault);
        }

        /**
         * Builds a fault when the processing time exceeded the configured threshold.
         * Returns {@code null} when the fault must not be stored.
         */
        private Fault buildProcessingTimeFault(long processingTime, String documentId) {
            if (processingTime > processingTimeThreshold) {
                Map<CharSequence, CharSequence> supplementaryData = new HashMap<>();
                supplementaryData.put(FAULT_SUPPLEMENTARY_DATA_PROCESSING_TIME, String.valueOf(processingTime));
                return Fault.newBuilder()
                        .setInputObjectId(documentId)
                        .setTimestamp(System.currentTimeMillis())
                        .setCode(FAULT_CODE_PROCESSING_TIME_THRESHOLD_EXCEEDED)
                        .setSupplementaryData(supplementaryData)
                        .build();
            }
            return null;
        }

        private void handleProcessingTime(long processingTime, String documentId, boolean storeProcessingTimeFault) {
            if (storeProcessingTimeFault) {
                Fault fault = buildProcessingTimeFault(processingTime, documentId);
                if (fault != null) {
                    log.warn("processing time threshold exceeded for id " + documentId);
                }
            }
            logFinished(documentId, processingTime);
        }

        /**
         * Parses content by relying on the embedded CERMINE library.
         */
        private ExtractedDocumentMetadata processWithCermine(String documentId, byte[] bytes)
                throws IOException, TimeoutException, AnalysisException, TransformationException {
            // disabling images extraction
            ExtractionConfigBuilder builder = new ExtractionConfigBuilder();
            builder.setProperty(ExtractionConfigProperty.IMAGES_EXTRACTION, false);
            ExtractionConfigRegister.set(builder.buildConfiguration());

            try (InputStream contentStream = new ByteBufferInputStream(ByteBuffer.wrap(bytes))) {
                ContentExtractor extractor = params.interruptProcessingTimeThresholdSecs != null
                        ? new ContentExtractor(params.interruptProcessingTimeThresholdSecs)
                        : new ContentExtractor();
                extractor.setPDF(contentStream);
                return handleContentWithCermine(extractor, documentId);
            }
        }

        private ExtractedDocumentMetadata handleContentWithCermine(ContentExtractor extractor, String documentId)
                throws TimeoutException, AnalysisException, IOException, TransformationException {
            Element resultElem = extractor.getContentAsNLM();
            Document doc = new Document(resultElem);
            String text = null;
            try {
                text = extractor.getRawFullText();
            } catch (AnalysisException e) {
                log.error("unable to extract plaintext, writing extracted metadata only", e);
            }
            return NlmToDocumentWithBasicMetadataConverter.convertFull(documentId, doc, text,
                    EXTRACTED_METADATA_RECORD_ORIGIN_CERMINE);
        }

        private void logProgress() {
            currentProgress++;
            if (currentProgress % 100 == 0) {
                log.info("metadata extraction progress: " + currentProgress + ", time taken to process 100 elements: "
                        + ((System.currentTimeMillis() - intervalTime) / 1000) + " secs");
                intervalTime = System.currentTimeMillis();
            }
        }

        private void logFinished(String documentId, long processingTime) {
            log.info("finished processing for id " + documentId + " in " + (processingTime / 1000) + " secs");
        }

        private void close() {
            exhausted = true;
            if (grobidClient != null) {
                try {
                    grobidClient.close();
                } catch (Exception e) {
                    log.warn("unable to close grobid client", e);
                }
            }
        }
    }

    //------------------------ PARAMETERS --------------------------

    /**
     * Parameters shared by the standalone job ({@link #main(String[])}) and the in-process
     * {@link #extract(JavaSparkContext, JavaRDD, Params)} used by the cache chain job.
     */
    @Parameters(separators = "=")
    public static class Params implements Serializable {

        private static final long serialVersionUID = 1L;

        @Parameter(names = "-input", required = true)
        public String input;

        @Parameter(names = "-outputRoot", required = true)
        public String outputRoot;

        @Parameter(names = "-outputNameMeta")
        public String outputNameMeta = "meta";

        @Parameter(names = "-outputNameFault")
        public String outputNameFault = "fault";

        @Parameter(names = "-outputNameTransientFault")
        public String outputNameTransientFault = "transientfault";

        @Parameter(names = "-outputReportPath")
        public String outputReportPath;

        @Parameter(names = "-excludedIds")
        public String excludedIds = UNDEFINED;

        @Parameter(names = "-interruptProcessingTimeThresholdSecs")
        public Integer interruptProcessingTimeThresholdSecs;

        @Parameter(names = "-logFaultProcessingTimeThresholdSecs")
        public Integer logFaultProcessingTimeThresholdSecs;

        @Parameter(names = "-grobidServerUrl")
        public String grobidServerUrl = UNDEFINED;

        @Parameter(names = "-grobidServerVersion")
        public String grobidServerVersion = UNDEFINED;

        @Parameter(names = "-grobidServerConnectionTimeout")
        public int grobidServerConnectionTimeout = 600000;

        @Parameter(names = "-grobidServerReadTimeout")
        public int grobidServerReadTimeout = 600000;

        @Parameter(names = "-grobidServerThrottleSleepTime")
        public long grobidServerThrottleSleepTime = 60000;

        @Parameter(names = "-grobidServerMaxRetriesCount")
        public int grobidServerMaxRetriesCount = 10;

        /**
         * Parses the CSV of identifiers excluded from processing, returning an empty set when
         * the parameter is undefined or blank.
         */
        public Set<String> excludedIds() {
            if (StringUtils.isBlank(excludedIds) || UNDEFINED.equals(excludedIds.trim())) {
                return Collections.emptySet();
            }
            return new HashSet<>(Arrays.asList(StringUtils.split(excludedIds.trim(), ',')));
        }

        /**
         * Processing time threshold expressed in milliseconds; {@link Long#MAX_VALUE} when disabled.
         */
        public long processingTimeThresholdMillis() {
            return logFaultProcessingTimeThresholdSecs == null
                    ? Long.MAX_VALUE
                    : SECS_TO_MILLIS * logFaultProcessingTimeThresholdSecs;
        }
    }
}
