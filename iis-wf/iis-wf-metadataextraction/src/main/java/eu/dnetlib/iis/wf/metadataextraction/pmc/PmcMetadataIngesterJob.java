package eu.dnetlib.iis.wf.metadataextraction.pmc;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.fault.FaultUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.wf.ingest.pmc.metadata.JatsXmlHandler;
import eu.dnetlib.iis.wf.ingest.pmc.metadata.MetadataImporter;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Spark port of {@link MetadataImporter} (the MapReduce mapper of the
 * {@code ingest/pmc/metadata} Oozie workflow).
 *
 * <p>Turns JATS/PMC XML records into PMC {@link eu.dnetlib.iis.ingest.pmc.metadata.schemas.
 * ExtractedDocumentMetadata} records plus faults. The parsing itself is reused from the original
 * mapper ({@link MetadataImporter#extractText}, {@link MetadataImporter#extractMetadata},
 * {@link JatsXmlHandler}), so this class only provides the Spark execution wrapper -- the same
 * relationship {@code MetadataExtractorJob} has with {@code MetadataExtractorMapper}.</p>
 *
 * <p>Failure semantics match the MapReduce mapper exactly: a record that cannot be parsed produces
 * an empty metadata record (required fields filled) AND a fault carrying the source XML under the
 * {@code text} key, so the record is never retried by later runs.</p>
 */
public class PmcMetadataIngesterJob {

    private static final Logger log = Logger.getLogger(PmcMetadataIngesterJob.class);

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    /** Report key prefix used by the Oozie {@code ingest/pmc/metadata} workflow. */
    public static final String DEFAULT_REPORT_PREFIX = "import.ingestPmc";

    public static final String FAULT_TEXT = MetadataImporter.FAULT_TEXT;

    private PmcMetadataIngesterJob() {
    }

    //------------------------ LOGIC --------------------------

    /**
     * Ingests the given {@link DocumentText} records. The returned RDDs are not cached and must be
     * consumed by exactly one action each; the counters are valid only afterwards.
     */
    public static IngestResult ingest(JavaSparkContext sc, JavaRDD<DocumentText> input, Params params) {
        LongAccumulator metaCounter = sc.sc().longAccumulator("PMC_META");
        LongAccumulator faultCounter = sc.sc().longAccumulator("PMC_FAULT");

        JavaRDD<IngestRecord> records = input
                .mapPartitions(partition -> new IngestPartitionIterator(partition, params, metaCounter,
                        faultCounter));
        // both outputs are derived from these records: cache them so the JATS parsing runs once
        records.cache();

        // one record per parsed document: either meta, or meta+fault for a failed one
        JavaRDD<eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata> meta = records
                .filter(record -> record.meta != null)
                .map(record -> record.meta);
        JavaRDD<Fault> fault = records
                .filter(record -> record.fault != null)
                .map(record -> record.fault);

        return new IngestResult(meta, fault, metaCounter, faultCounter);
    }

    /** Report entries of a completed ingestion; call only after an action has run. */
    public static List<ReportEntry> reportEntries(IngestResult result, String prefix) {
        return Lists.newArrayList(
                ReportEntryFactory.createCounterReportEntry(prefix + ".processed.docMetadata",
                        result.metaCounter.value()),
                ReportEntryFactory.createCounterReportEntry(prefix + ".processed.fault",
                        result.faultCounter.value()));
    }

    public static void main(String[] args) throws Exception {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = eu.dnetlib.iis.common.spark.JavaSparkContextFactory
                .withConfAndKryo(new org.apache.spark.SparkConf())) {
            HdfsRemove.remove(sc, params.outputRoot);

            JavaRDD<DocumentText> input = avroLoader.loadJavaRDD(sc, params.input, DocumentText.class);
            IngestResult result = ingest(sc, input, params);

            avroSaver.saveJavaRDD(result.meta,
                    eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata.SCHEMA$,
                    params.outputRoot + "/" + params.outputNameMeta);
            avroSaver.saveJavaRDD(result.fault, Fault.SCHEMA$,
                    params.outputRoot + "/" + params.outputNameFault);

            if (StringUtils.isNotBlank(params.outputReportRootPath)) {
                avroSaver.saveJavaRDD(sc.parallelize(reportEntries(result, params.reportPropertiesPrefix))
                        .repartition(1), ReportEntry.SCHEMA$,
                        params.outputReportRootPath + "/" + params.outputReportRelativePath);
            }
        }
    }

    /** Minimal indirection to keep the main method readable. */
    private static final class HdfsRemove {
        static void remove(JavaSparkContext sc, String path) throws java.io.IOException {
            eu.dnetlib.iis.common.java.io.HdfsUtils.remove(sc.hadoopConfiguration(), path);
        }
    }

    //------------------------ PRIVATE --------------------------

    /** Meta and/or fault produced for a single input record. */
    static final class IngestRecord implements Serializable {

        private static final long serialVersionUID = 1L;

        final eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata meta;
        final Fault fault;

        IngestRecord(eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata meta,
                Fault fault) {
            this.meta = meta;
            this.fault = fault;
        }
    }

    /**
     * Per-partition ingestion iterator. Records excluded by id or carrying blank text are skipped
     * silently, exactly like the MapReduce mapper.
     */
    static class IngestPartitionIterator implements Iterator<IngestRecord>, Serializable {

        private static final long serialVersionUID = 1L;

        private final List<DocumentText> texts;
        private final Set<String> excludedIds;
        private final org.jdom.Namespace oaiNamespace;
        private final LongAccumulator metaCounter;
        private final LongAccumulator faultCounter;

        private int index;
        private IngestRecord next;

        IngestPartitionIterator(Iterator<DocumentText> partition, Params params,
                LongAccumulator metaCounter, LongAccumulator faultCounter) {
            this.texts = Lists.newArrayList(partition);
            this.excludedIds = params.excludedIds();
            this.oaiNamespace = params.oaiNamespace();
            this.metaCounter = metaCounter;
            this.faultCounter = faultCounter;
        }

        @Override
        public boolean hasNext() {
            if (next == null) {
                next = fetchNext();
            }
            return next != null;
        }

        @Override
        public IngestRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            IngestRecord result = next;
            next = null;
            return result;
        }

        private IngestRecord fetchNext() {
            while (index < texts.size()) {
                IngestRecord record = process(texts.get(index++));
                if (record != null) {
                    return record;
                }
            }
            return null;
        }

        private IngestRecord process(DocumentText documentText) {
            String documentId = documentText.getId().toString();
            if (excludedIds.contains(documentId)) {
                log.info("skipping processing for excluded id " + documentId);
                return null;
            }
            if (StringUtils.isBlank(documentText.getText())) {
                return null;
            }

            final eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata.Builder output =
                    eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata.newBuilder();
            output.setId(documentText.getId());
            String pmcXml = documentText.getText().toString();
            try {
                output.setText(MetadataImporter.extractText(pmcXml, oaiNamespace));
                MetadataImporter.extractMetadata(pmcXml, output);
                metaCounter.add(1);
                return new IngestRecord(output.build(), null);
            } catch (Exception e) {
                log.error("unexpected exception occured while ingesting PMC record " + documentId, e);
                // parity with MetadataImporter.handleException: empty meta record (so the document
                // is not retried) plus a fault carrying the source xml
                if (!output.hasText()) {
                    output.setText("");
                }
                if (!output.hasEntityType()) {
                    output.setEntityType(JatsXmlHandler.ENTITY_TYPE_UNKNOWN);
                }
                Map<CharSequence, CharSequence> supplementaryData = new HashMap<>();
                supplementaryData.put(FAULT_TEXT, documentText.getText());
                metaCounter.add(1);
                faultCounter.add(1);
                return new IngestRecord(output.build(),
                        FaultUtils.exceptionToFault(documentText.getId(), e, supplementaryData));
            }
        }
    }

    //------------------------ PARAMETERS --------------------------

    @Parameters(separators = "=")
    public static class Params implements Serializable {

        private static final long serialVersionUID = 1L;

        @Parameter(names = "-input", required = true)
        public String input;

        @Parameter(names = "-outputRoot")
        public String outputRoot;

        @Parameter(names = "-outputNameMeta")
        public String outputNameMeta = "meta";

        @Parameter(names = "-outputNameFault")
        public String outputNameFault = "fault";

        @Parameter(names = "-excludedIds")
        public String excludedIds = eu.dnetlib.iis.common.WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE;

        @Parameter(names = "-reportPropertiesPrefix")
        public String reportPropertiesPrefix = DEFAULT_REPORT_PREFIX;

        @Parameter(names = "-outputReportRootPath")
        public String outputReportRootPath;

        @Parameter(names = "-outputReportRelativePath")
        public String outputReportRelativePath = "import_ingest_pmc";

        @Parameter(names = "-ingestMetadataOaiNamespace")
        public String ingestMetadataOaiNamespace = "http://www.openarchives.org/OAI/2.0/";

        private transient org.jdom.Namespace oaiNamespace;

        /** Set of identifiers excluded from processing, mirroring the MapReduce mapper. */
        public Set<String> excludedIds() {
            if (excludedIds == null || excludedIds.trim().isEmpty()
                    || eu.dnetlib.iis.common.WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE
                            .equals(excludedIds)) {
                return Collections.emptySet();
            }
            return new HashSet<>(Arrays.asList(StringUtils.split(excludedIds.trim(), ',')));
        }

        public org.jdom.Namespace oaiNamespace() {
            if (oaiNamespace == null) {
                oaiNamespace = org.jdom.Namespace.getNamespace(ingestMetadataOaiNamespace);
            }
            return oaiNamespace;
        }
    }

    /**
     * PMC metadata (pre-transformation schema) and faults produced by a single {@link #ingest} call.
     */
    public static final class IngestResult implements Serializable {

        private static final long serialVersionUID = 1L;

        public final JavaRDD<eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata> meta;
        public final JavaRDD<Fault> fault;
        public final LongAccumulator metaCounter;
        public final LongAccumulator faultCounter;

        IngestResult(JavaRDD<eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata> meta,
                JavaRDD<Fault> fault, LongAccumulator metaCounter, LongAccumulator faultCounter) {
            this.meta = meta;
            this.fault = fault;
            this.metaCounter = metaCounter;
            this.faultCounter = faultCounter;
        }
    }
}
