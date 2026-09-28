package eu.dnetlib.iis.wf.importer.content;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;
import eu.dnetlib.iis.importer.schemas.DocumentContent;
import eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Spark port of {@link DocumentContentUrlBasedImporterMapper}.
 *
 * <p>Retrieves content bytes for every {@link DocumentContentUrl} record producing a
 * {@link DocumentContent} datastore. This is the port of the Oozie
 * {@code eu/dnetlib/iis/wf/importer/content/oozie_app} workflow which is the content retrieval
 * phase referenced by the {@code metadataextraction/prefetched} workflow.</p>
 *
 * <p>Content is retrieved from the object store via
 * {@link ObjectStoreContentProviderUtils#getContentFromURL(String, ContentRetrievalContext)}, so
 * the S3 endpoint and credentials are resolved exactly as in the MapReduce implementation
 * (including the {@code hadoop.security.credential.provider.path} mechanism).</p>
 *
 * <p>Records are skipped when: the declared content size is not greater than 0, the declared
 * content size exceeds {@code -maxFileSizeMb}, or content retrieval fails. Such records are
 * counted and reported but do not fail the job.</p>
 */
public class DocumentContentUrlBasedImporterJob {

    private static final Logger log = Logger.getLogger(DocumentContentUrlBasedImporterJob.class);

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    // Report entry keys mirroring the Oozie ReportGenerator properties of the importer_content workflow.
    public static final String REPORT_KEY_CORRECT = "import.contents.pdf.correct";
    public static final String REPORT_KEY_INVALID_SIZE_EXCEEDED = "import.contents.pdf.invalid.sizeExceeded";
    public static final String REPORT_KEY_INVALID_SIZE = "import.contents.pdf.invalid.sizeInvalid";
    public static final String REPORT_KEY_INVALID_UNAVAILABLE = "import.contents.pdf.invalid.unavailable";

    private DocumentContentUrlBasedImporterJob() {
    }

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws Exception {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            Configuration hadoopConf = sc.hadoopConfiguration();

            HdfsUtils.remove(hadoopConf, params.output);
            HdfsUtils.remove(hadoopConf, params.outputReportPath);

            LongAccumulator sizeExceededCounter = sc.sc().longAccumulator("SIZE_EXCEEDED");
            LongAccumulator sizeInvalidCounter = sc.sc().longAccumulator("SIZE_INVALID");
            LongAccumulator unavailableCounter = sc.sc().longAccumulator("UNAVAILABLE");
            LongAccumulator correctCounter = sc.sc().longAccumulator("CORRECT");

            JavaRDD<DocumentContentUrl> input = avroLoader.loadJavaRDD(sc, params.input, DocumentContentUrl.class);

            JavaRDD<DocumentContent> output = input
                    .mapPartitions(partition -> new ContentRetrievalIterator(partition, params,
                            sizeExceededCounter, sizeInvalidCounter, unavailableCounter, correctCounter));

            long correctCount = output.count();

            JavaRDD<ReportEntry> reports = sc.parallelize(Lists.newArrayList(
                    ReportEntryFactory.createCounterReportEntry(REPORT_KEY_CORRECT, correctCount),
                    ReportEntryFactory.createCounterReportEntry(REPORT_KEY_INVALID_SIZE_EXCEEDED,
                            sizeExceededCounter.value()),
                    ReportEntryFactory.createCounterReportEntry(REPORT_KEY_INVALID_SIZE,
                            sizeInvalidCounter.value()),
                    ReportEntryFactory.createCounterReportEntry(REPORT_KEY_INVALID_UNAVAILABLE,
                            unavailableCounter.value())));

            avroSaver.saveJavaRDD(output, DocumentContent.SCHEMA$, params.output);
            if (params.outputReportPath != null) {
                avroSaver.saveJavaRDD(reports.repartition(1), ReportEntry.SCHEMA$, params.outputReportPath);
            }
        }
    }

    /**
     * Builds the retrieval context inside a Spark task. A fresh {@link Configuration} is used
     * because the driver's HDFS {@link Configuration} is not serializable and would break the
     * Spark closure; classpath configuration files (core-site.xml/hdfs-site.xml) mounted into the
     * pod are still picked up. All retrieval parameters are taken from the serializable
     * {@link Params}.
     */
    private static ContentRetrievalContext createRetrievalContext(Params params) {
        Configuration conf = new Configuration();
        conf.setInt(ImportWorkflowRuntimeParameters.IMPORT_CONTENT_CONNECTION_TIMEOUT,
                params.contentConnectionTimeout);
        conf.setInt(ImportWorkflowRuntimeParameters.IMPORT_CONTENT_READ_TIMEOUT,
                params.contentReadTimeout);
        if (isDefined(params.maxFileSizeMb)) {
            conf.set(ImportWorkflowRuntimeParameters.IMPORT_CONTENT_MAX_FILE_SIZE_MB, params.maxFileSizeMb);
        }
        if (isDefined(params.objectstoreS3Endpoint)) {
            conf.set(ImportWorkflowRuntimeParameters.IMPORT_CONTENT_OBJECT_STORE_S3_ENDPOINT,
                    params.objectstoreS3Endpoint);
        }
        if (isDefined(params.objectstoreS3KeystoreLocation)) {
            conf.set("hadoop.security.credential.provider.path", params.objectstoreS3KeystoreLocation);
        }
        return new ContentRetrievalContext(conf);
    }

    private static boolean isDefined(String value) {
        return value != null && !"$UNDEFINED$".equals(value);
    }

    //------------------------ PRIVATE --------------------------

    /**
     * Per-partition content retrieval iterator. Only the (small) {@link DocumentContentUrl}
     * metadata is buffered, whereas the (potentially large) content bytes of a single record are
     * fetched lazily one at a time.
     */
    static class ContentRetrievalIterator implements Iterator<DocumentContent>, Serializable {

        private static final long serialVersionUID = 1L;

        private final List<DocumentContentUrl> urls;
        private final transient ContentRetrievalContext contentRetrievalContext;
        private final LongAccumulator sizeExceededCounter;
        private final LongAccumulator sizeInvalidCounter;
        private final LongAccumulator unavailableCounter;
        private final LongAccumulator correctCounter;

        private int index;
        private DocumentContent nextContent;

        ContentRetrievalIterator(Iterator<DocumentContentUrl> partition, Params params,
                LongAccumulator sizeExceededCounter, LongAccumulator sizeInvalidCounter,
                LongAccumulator unavailableCounter, LongAccumulator correctCounter) {
            this.urls = Lists.newArrayList(partition);
            this.sizeExceededCounter = sizeExceededCounter;
            this.sizeInvalidCounter = sizeInvalidCounter;
            this.unavailableCounter = unavailableCounter;
            this.correctCounter = correctCounter;
            // the S3 client held by the context is not serializable -> instantiate lazily inside the task
            this.contentRetrievalContext = createRetrievalContext(params);
        }

        @Override
        public boolean hasNext() {
            if (nextContent == null) {
                nextContent = fetchNext();
            }
            return nextContent != null;
        }

        @Override
        public DocumentContent next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            DocumentContent result = nextContent;
            nextContent = null;
            return result;
        }

        private DocumentContent fetchNext() {
            while (index < urls.size()) {
                DocumentContent content = process(urls.get(index++));
                if (content != null) {
                    return content;
                }
            }
            return null;
        }

        private DocumentContent process(DocumentContentUrl docUrl) {
            if (docUrl.getContentSizeKB() <= 0) {
                log.warn("content " + docUrl.getId() + " discarded for location: " + docUrl.getUrl()
                        + " and size [kB]: " + docUrl.getContentSizeKB()
                        + ", size is expected to be greater than 0!");
                sizeInvalidCounter.add(1);
                return null;
            }
            if (docUrl.getContentSizeKB() > contentRetrievalContext.getMaxFileSizeKB()) {
                log.info("content " + docUrl.getId() + " discarded for location: " + docUrl.getUrl()
                        + " and size [kB]: " + docUrl.getContentSizeKB() + ", size limit: "
                        + contentRetrievalContext.getMaxFileSizeKB() + " exceeded!");
                sizeExceededCounter.add(1);
                return null;
            }

            long startTime = System.currentTimeMillis();
            log.info("starting content retrieval for id: " + docUrl.getId() + ", location: " + docUrl.getUrl()
                    + " and size [kB]: " + docUrl.getContentSizeKB());
            try {
                byte[] content = ObjectStoreContentProviderUtils.getContentFromURL(docUrl.getUrl().toString(),
                        contentRetrievalContext);
                log.info("content retrieval for id: " + docUrl.getId() + " took: "
                        + (System.currentTimeMillis() - startTime) + " ms");
                correctCounter.add(1);
                return DocumentContent.newBuilder()
                        .setId(docUrl.getId())
                        .setPdf(ByteBuffer.wrap(content))
                        .build();
            } catch (S3EndpointNotFoundException e) {
                throw new IllegalStateException("Got S3 link: " + docUrl.getUrl()
                        + " but no S3 endpoint was specified in job configuration!", e);
            } catch (InvalidSizeException e) {
                log.warn("content " + docUrl.getId() + " discarded for location: " + docUrl.getUrl()
                        + ", real size is expected to be greater than 0!");
                sizeInvalidCounter.add(1);
                return null;
            } catch (IOException e) {
                log.error("unexpected exception occured while obtaining content " + docUrl.getId()
                        + " for location: " + docUrl.getUrl(), e);
                unavailableCounter.add(1);
                return null;
            }
        }
    }

    //------------------------ PARAMETERS --------------------------

    @Parameters(separators = "=")
    public static class Params implements Serializable {

        private static final long serialVersionUID = 1L;

        @Parameter(names = "-input", required = true)
        public String input;

        @Parameter(names = "-output", required = true)
        public String output;

        @Parameter(names = "-outputReportPath")
        public String outputReportPath;

        @Parameter(names = "-maxFileSizeMb")
        public String maxFileSizeMb = "$UNDEFINED$";

        @Parameter(names = "-contentConnectionTimeout")
        public int contentConnectionTimeout = 60000;

        @Parameter(names = "-contentReadTimeout")
        public int contentReadTimeout = 60000;

        @Parameter(names = "-objectstoreS3Endpoint")
        public String objectstoreS3Endpoint = "$UNDEFINED$";

        @Parameter(names = "-objectstoreS3KeystoreLocation")
        public String objectstoreS3KeystoreLocation = "$UNDEFINED$";
    }
}
