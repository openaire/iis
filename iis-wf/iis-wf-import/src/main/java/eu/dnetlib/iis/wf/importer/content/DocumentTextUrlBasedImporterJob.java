package eu.dnetlib.iis.wf.importer.content;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Spark port of {@link DocumentTextUrlBasedImporterMapper}.
 *
 * <p>Retrieves the text content referenced by every {@link DocumentContentUrl} (for the PMC/JATS
 * path this is the XML record itself) and decodes it into a {@link DocumentText} datastore. This is
 * the counterpart of {@link DocumentContentUrlBasedImporterJob} for the XML ingestion workflow:</p>
 *
 * <pre>
 *   PDF path : DocumentContentUrl -> DocumentContent (bytes)  -> Grobid/CERMINE extraction
 *   PMC path : DocumentContentUrl -> DocumentText    (chars)  -> JATS ingestion
 * </pre>
 *
 * <p>The retrieval machinery (object store endpoint, credentials, size limits, failure handling) is
 * the shared one: the {@link ContentRetrievalContext} factory and the parameter object are taken
 * from {@link DocumentContentUrlBasedImporterJob}, so both importers behave identically. As in the
 * MapReduce mapper, every retrieval failure other than a missing S3 endpoint is counted as
 * "unavailable" and does not fail the job.</p>
 */
public class DocumentTextUrlBasedImporterJob {

    private static final Logger log = Logger.getLogger(DocumentTextUrlBasedImporterJob.class);

    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    /** Report key prefix of the PMC/JATS importer; the PDF one is {@code import.contents.pdf}. */
    public static final String DEFAULT_REPORT_PREFIX = "import.contents.jats";

    //------------------------ LOGIC --------------------------

    /**
     * Retrieves the text content of the given urls. The returned RDD is not cached and must be
     * consumed by exactly one action; the counters are valid only afterwards.
     */
    public static TextImportResult importText(JavaSparkContext sc, JavaRDD<DocumentContentUrl> input,
            DocumentContentUrlBasedImporterJob.Params params) {
        if (params.hadoopConf == null) {
            params.hadoopConf = DocumentContentUrlBasedImporterJob.toMap(sc.hadoopConfiguration());
        }
        LongAccumulator sizeExceededCounter = sc.sc().longAccumulator("TEXT_SIZE_EXCEEDED");
        LongAccumulator sizeInvalidCounter = sc.sc().longAccumulator("TEXT_SIZE_INVALID");
        LongAccumulator unavailableCounter = sc.sc().longAccumulator("TEXT_UNAVAILABLE");
        LongAccumulator correctCounter = sc.sc().longAccumulator("TEXT_CORRECT");

        JavaRDD<DocumentText> text = input
                .mapPartitions(partition -> new TextRetrievalIterator(partition, params,
                        sizeExceededCounter, sizeInvalidCounter, unavailableCounter, correctCounter));
        return new TextImportResult(text, sizeExceededCounter, sizeInvalidCounter, unavailableCounter,
                correctCounter);
    }

    /**
     * Builds the report entries of a completed import. Must be called only after an action
     * consuming {@code result.text} has finished.
     *
     * @param prefix report key prefix, e.g. {@code import.contents.jats} for the PMC path
     */
    public static List<ReportEntry> reportEntries(TextImportResult result, String prefix) {
        return Lists.newArrayList(
                ReportEntryFactory.createCounterReportEntry(prefix + ".correct",
                        result.correctCounter.value()),
                ReportEntryFactory.createCounterReportEntry(prefix + ".invalid.sizeExceeded",
                        result.sizeExceededCounter.value()),
                ReportEntryFactory.createCounterReportEntry(prefix + ".invalid.sizeInvalid",
                        result.sizeInvalidCounter.value()),
                ReportEntryFactory.createCounterReportEntry(prefix + ".invalid.unavailable",
                        result.unavailableCounter.value()));
    }

    /** Persists the retrieved text; used by the standalone entry point. */
    public static void save(JavaSparkContext sc, TextImportResult result, String output) {
        avroSaver.saveJavaRDD(result.text, DocumentText.SCHEMA$, output);
    }

    //------------------------ PRIVATE --------------------------

    /**
     * Per-partition retrieval iterator. Only the (small) url metadata is buffered, the text of a
     * single record is fetched lazily.
     */
    static class TextRetrievalIterator implements Iterator<DocumentText>, Serializable {

        private static final long serialVersionUID = 1L;

        private final List<DocumentContentUrl> urls;
        private final transient ContentRetrievalContext contentRetrievalContext;
        private final LongAccumulator sizeExceededCounter;
        private final LongAccumulator sizeInvalidCounter;
        private final LongAccumulator unavailableCounter;
        private final LongAccumulator correctCounter;

        private int index;
        private DocumentText nextText;

        TextRetrievalIterator(Iterator<DocumentContentUrl> partition,
                DocumentContentUrlBasedImporterJob.Params params,
                LongAccumulator sizeExceededCounter, LongAccumulator sizeInvalidCounter,
                LongAccumulator unavailableCounter, LongAccumulator correctCounter) {
            this.urls = Lists.newArrayList(partition);
            this.sizeExceededCounter = sizeExceededCounter;
            this.sizeInvalidCounter = sizeInvalidCounter;
            this.unavailableCounter = unavailableCounter;
            this.correctCounter = correctCounter;
            // the S3 client held by the context is not serializable -> instantiate inside the task
            this.contentRetrievalContext = DocumentContentUrlBasedImporterJob.createRetrievalContext(params);
        }

        @Override
        public boolean hasNext() {
            if (nextText == null) {
                nextText = fetchNext();
            }
            return nextText != null;
        }

        @Override
        public DocumentText next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            DocumentText result = nextText;
            nextText = null;
            return result;
        }

        private DocumentText fetchNext() {
            while (index < urls.size()) {
                DocumentText text = process(urls.get(index++));
                if (text != null) {
                    return text;
                }
            }
            return null;
        }

        private DocumentText process(DocumentContentUrl docUrl) {
            if (docUrl.getContentSizeKB() <= 0) {
                log.warn("content " + docUrl.getId() + " discarded for location: " + docUrl.getUrl()
                        + " and size [kB]: " + docUrl.getContentSizeKB()
                        + ", size is expected to be greater than 0!");
                sizeInvalidCounter.add(1);
                return null;
            }
            if (docUrl.getContentSizeKB() > contentRetrievalContext.getMaxFileSizeKB()) {
                log.warn("skipping processing for id " + docUrl.getId() + " due to max file size limit="
                        + contentRetrievalContext.getMaxFileSizeKB() + " KB exceeded: "
                        + docUrl.getContentSizeKB() + " KB");
                sizeExceededCounter.add(1);
                return null;
            }

            long startTime = System.currentTimeMillis();
            log.info("starting text content retrieval for id: " + docUrl.getId() + ", location: "
                    + docUrl.getUrl() + " and size [kB]: " + docUrl.getContentSizeKB());
            try {
                byte[] content = ObjectStoreContentProviderUtils.getContentFromURL(
                        docUrl.getUrl().toString(), contentRetrievalContext);
                log.info("text content retrieval for id: " + docUrl.getId() + " took: "
                        + (System.currentTimeMillis() - startTime) + " ms");
                correctCounter.add(1);
                return DocumentText.newBuilder()
                        .setId(docUrl.getId())
                        .setText(new String(content, ObjectStoreContentProviderUtils.defaultEncoding))
                        .build();
            } catch (S3EndpointNotFoundException e) {
                throw new IllegalStateException("Got S3 link: " + docUrl.getUrl()
                        + " but no S3 endpoint was specified in job configuration!", e);
            } catch (InvalidSizeException e) {
                log.warn("content " + docUrl.getId() + " discarded for location: " + docUrl.getUrl()
                        + ", real size is expected to be greater than 0!");
                sizeInvalidCounter.add(1);
                return null;
            } catch (Exception e) {
                // parity with DocumentTextUrlBasedImporterMapper: any retrieval failure is counted
                // as unavailable and must not fail the job
                log.error("unexpected exception occured while obtaining content " + docUrl.getId()
                        + " for location: " + docUrl.getUrl(), e);
                unavailableCounter.add(1);
                return null;
            }
        }
    }

    /**
     * Retrieved text plus the retrieval counters of a single {@link #importText} call.
     */
    public static final class TextImportResult implements Serializable {

        private static final long serialVersionUID = 1L;

        public final JavaRDD<DocumentText> text;
        public final LongAccumulator sizeExceededCounter;
        public final LongAccumulator sizeInvalidCounter;
        public final LongAccumulator unavailableCounter;
        public final LongAccumulator correctCounter;

        TextImportResult(JavaRDD<DocumentText> text, LongAccumulator sizeExceededCounter,
                LongAccumulator sizeInvalidCounter, LongAccumulator unavailableCounter,
                LongAccumulator correctCounter) {
            this.text = text;
            this.sizeExceededCounter = sizeExceededCounter;
            this.sizeInvalidCounter = sizeInvalidCounter;
            this.unavailableCounter = unavailableCounter;
            this.correctCounter = correctCounter;
        }
    }
}
