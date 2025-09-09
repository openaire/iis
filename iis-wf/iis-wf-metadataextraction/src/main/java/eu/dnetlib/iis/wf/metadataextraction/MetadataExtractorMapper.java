package eu.dnetlib.iis.wf.metadataextraction;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.avro.mapred.AvroKey;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.jdom.Document;
import org.jdom.Element;

import com.itextpdf.text.exceptions.InvalidPdfException;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.fault.FaultUtils;
import eu.dnetlib.iis.common.javamapreduce.MultipleOutputs;
import eu.dnetlib.iis.importer.schemas.DocumentContent;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.wf.importer.content.approver.ContentApprover;
import eu.dnetlib.iis.wf.importer.content.approver.InvalidCountableContentApproverWrapper;
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

/**
 * Metadata extractor module.
 * 
 * @author Mateusz Kobos
 * @author mhorst
 *
 */
public class MetadataExtractorMapper extends Mapper<AvroKey<DocumentContent>, NullWritable, NullWritable, NullWritable> {

    public static final String NAMED_OUTPUT_META = "output.meta";
    
    public static final String NAMED_OUTPUT_FAULT = "output.fault";
    
    public static final String EXCLUDED_IDS = "excluded.ids";
    
    public static final String GROBID_SERVER_URL = "grobid.server.url";
    
    public static final String GROBID_SERVER_VERSION = "grobid.server.version";
    
    public static final String GROBID_SERVER_READ_TIMEOUT = "grobid.server.read.timeout";
    
    public static final String GROBID_SERVER_CONNECTION_TIMEOUT = "grobid.server.connection.timeout";
    
    public static final String LOG_FAULT_PROCESSING_TIME_THRESHOLD_SECS = "log.fault.processing.time.threshold.secs";
    
    public static final String INTERRUPT_PROCESSING_TIME_THRESHOLD_SECS = "interrupt.processing.time.threshold.secs";

    public static final String FAULT_CODE_PROCESSING_TIME_THRESHOLD_EXCEEDED = "ProcessingTimeThresholdExceeded";

    public static final String FAULT_SUPPLEMENTARY_DATA_PROCESSING_TIME = "processing_time";

    public static final String EXTRACTED_METADATA_RECORD_ORIGIN_CERMINE = "CERMINE";
    
    public static final String EXTRACTED_METADATA_RECORD_ORIGIN_GROBID = "GROBID";
    
    public static final String EXTRACTED_METADATA_RECORD_ORIGIN_UNSPECIFIED = "UNSPECIFIED";
    
    
    protected static final Logger log = Logger.getLogger(MetadataExtractorMapper.class);

    /**
     * Progress log interval.
     */
    private static final int PROGRESS_LOG_INTERVAL = 100;
    
    private static final long SECS_TO_MILLIS = 1000l;
    
    /**
     * Multiple outputs.
     */
    private MultipleOutputs mos;

    /**
     * Document metadata named output.
     */
    private String namedOutputMeta;

    /**
     * Fault named output.
     */
    private String namedOutputFault;

    /**
     * Current progress.
     */
    private int currentProgress = 0;

    /**
     * Interval time.
     */
    private long intervalTime = 0;

    /**
     * Processing timeout threshold, CERMINE metadata extraction for given record will be interrupted when threshold exceeded.
     */
    private Integer interruptionTimeoutSecs;
    
    /**
     * Processing time threshold. When exceeded apropriate object will be
     * written to error datastore.
     */
    private long processingTimeThreshold = Long.MAX_VALUE;

    /**
     * Set of object identifiers objects excluded from processing.
     */
    private Set<String> excludedIds = Collections.emptySet();

    /**
     * Content approver module.
     */
    private ContentApprover contentApprover;
    
    /**
     * Grobid client responsible for communication with external grobid server.
     */
    private GrobidClient grobidClient;
    
    /**
     * Grobid server version.
     */
    private String grobidServerVersion;
    
    /**
     * Hadoop counters enum of invalid records 
     */
    public static enum InvalidRecordCounters {
        INVALID_PDF_HEADER
    }
    
    private static final String invalidPdfHeaderMsg = "content PDF header not approved!";
    
    //------------------------ LOGIC --------------------------
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        namedOutputMeta = context.getConfiguration().get(NAMED_OUTPUT_META);
        if (namedOutputMeta == null || namedOutputMeta.isEmpty()) {
            throw new RuntimeException("no named output provided for metadata");
        }
        namedOutputFault = context.getConfiguration().get(NAMED_OUTPUT_FAULT);
        if (namedOutputFault == null || namedOutputFault.isEmpty()) {
            throw new RuntimeException("no named output provided for fault");
        }
        
        String grobidServerUrl = context.getConfiguration().get(GROBID_SERVER_URL);
        if (grobidServerUrl != null && !grobidServerUrl.trim().isEmpty()
                && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(grobidServerUrl)) {
            log.info("enabling metadata extraction relying on Grobid, url address: " + grobidServerUrl);
            int connectionTimeout = 60000;
            String connectionTimeoutStr = context.getConfiguration().get(GROBID_SERVER_CONNECTION_TIMEOUT);
            if (connectionTimeoutStr != null && !connectionTimeoutStr.trim().isEmpty() && 
                    !!WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(connectionTimeoutStr)) {
                connectionTimeout = Integer.parseInt(connectionTimeoutStr); 
            }
            int readTimeout = 60000;
            String readTimeoutStr = context.getConfiguration().get(GROBID_SERVER_READ_TIMEOUT);
            if (readTimeoutStr != null && !readTimeoutStr.trim().isEmpty() && 
                    !!WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(readTimeoutStr)) {
                readTimeout = Integer.parseInt(readTimeoutStr); 
            }
            this.grobidClient = new GrobidClient(grobidServerUrl, connectionTimeout, readTimeout);
            
            String grobidServerVersionStr = context.getConfiguration().get(GROBID_SERVER_VERSION);
            if (grobidServerVersionStr != null && !grobidServerVersionStr.trim().isEmpty()
                    && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(grobidServerVersionStr)) {
                this.grobidServerVersion = grobidServerVersionStr.trim();
            } else {
                this.grobidServerVersion = EXTRACTED_METADATA_RECORD_ORIGIN_GROBID;
            }
            
        } else {
            log.info("enabling metadata extraction relying on CERMINE");
        }

        String excludedIdsCSV = context.getConfiguration().get(EXCLUDED_IDS);
        if (excludedIdsCSV != null && !excludedIdsCSV.trim().isEmpty()
                && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(excludedIdsCSV)) {
            log.info("got excluded ids: " + excludedIdsCSV);
            excludedIds = new HashSet<String>(Arrays.asList(StringUtils.split(excludedIdsCSV.trim(), ',')));
        } else {
            log.info("got no excluded ids");
        }
        // handling processing time threshold: interruption and fault logging
        interruptionTimeoutSecs = WorkflowRuntimeParameters.getIntegerParamValue(
                INTERRUPT_PROCESSING_TIME_THRESHOLD_SECS, context.getConfiguration());
        Integer processingTimeThresholdSecs = WorkflowRuntimeParameters.getIntegerParamValue(
                LOG_FAULT_PROCESSING_TIME_THRESHOLD_SECS, context.getConfiguration());
        if (processingTimeThresholdSecs != null) {
            this.processingTimeThreshold = SECS_TO_MILLIS * processingTimeThresholdSecs;
        }

        Counter invalidPdfCounter = context.getCounter(InvalidRecordCounters.INVALID_PDF_HEADER);
        invalidPdfCounter.setValue(0);
        this.contentApprover = new InvalidCountableContentApproverWrapper(new PDFHeaderBasedContentApprover(), invalidPdfCounter);
        
        mos = instantiateMultipleOutputs(context);
        currentProgress = 0;
        intervalTime = System.currentTimeMillis();
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
        if (this.grobidClient != null) {
            this.grobidClient.close();    
        }
    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    public void map(AvroKey<DocumentContent> key, NullWritable ignore, Context context)
            throws IOException, InterruptedException {
        DocumentContent content = key.datum();
        String documentId = content.getId().toString();
        
        if (excludedIds.contains(documentId)) {
            log.info("skipping processing for excluded id " + documentId);
            return;
        }
        
        if (content.getPdf()!=null) {
            ByteBuffer byteBuffer = content.getPdf();
            if (byteBuffer.hasArray() && contentApprover.approve(byteBuffer.array())) {
                try (InputStream inputStream = new ByteBufferInputStream(byteBuffer)) {
                    processStream(documentId, inputStream);
                }    
            } else {
                log.info(invalidPdfHeaderMsg);
                handleException(new InvalidPdfException(invalidPdfHeaderMsg), content.getId().toString(),
                        EXTRACTED_METADATA_RECORD_ORIGIN_UNSPECIFIED);
            }
        } else {
            log.warn("no byte data found for id: " + content.getId());
        }
    }
    
    /**
     * Instantiates {@link MultipleOutputs} instance.
     */
    protected MultipleOutputs instantiateMultipleOutputs(Context context) {
        return new MultipleOutputs(context);
    }
    
    /**
     * Processes content input stream. Does not close contentStream.
     * 
     * @param documentId document identifier
     * @param contentStream stream to be processed
     */
    protected void processStream(String documentId, InputStream contentStream) throws IOException, InterruptedException {
        currentProgress++;
        if (currentProgress % PROGRESS_LOG_INTERVAL == 0) {
            log.info("metadata extaction progress: " + currentProgress + ", time taken to process "
                    + PROGRESS_LOG_INTERVAL + " elements: " + ((System.currentTimeMillis() - intervalTime) / 1000)
                    + " secs");
            intervalTime = System.currentTimeMillis();
        }
        
        log.info("starting processing for id: " + documentId);
        long startTime = System.currentTimeMillis();
        String extractedBy = "";
        try {
            if (grobidClient != null) {
                extractedBy = grobidServerVersion;
                processStreamWithGrobid(documentId, contentStream);
            } else {
                extractedBy = EXTRACTED_METADATA_RECORD_ORIGIN_CERMINE;
                processStreamWithCermine(documentId, contentStream); 
            }
        } catch (Exception e) {
            handleException(e, documentId, extractedBy);
            return;
        }
        
        handleProcessingTime(System.currentTimeMillis() - startTime, documentId);
    }

    
    //------------------------ PRIVATE --------------------------

    /**
     * Parses content stream by relying on external Grobid server.
     * @param documentId source document identifier
     * @param contentStream PDF content stream to be processed
     */
    private void processStreamWithGrobid(String documentId, InputStream contentStream)  throws Exception {
        String teiXml = grobidClient.processPdfInputStream(contentStream);
        mos.write(namedOutputMeta, new AvroKey<ExtractedDocumentMetadata>(
                TeiToExtractedDocumentMetadataTransformer.transformToExtractedDocumentMetadata(documentId, teiXml, grobidServerVersion)));
    }
    
    /**
     * Parses content stream by relying on embedded CERMINE library.
     * @param documentId source document identifier
     * @param contentStream PDF content stream to be processed
     */
    private void processStreamWithCermine(String documentId, InputStream contentStream)  throws Exception {
     // disabling images extraction
        ExtractionConfigBuilder builder = new ExtractionConfigBuilder(); 
        builder.setProperty(ExtractionConfigProperty.IMAGES_EXTRACTION, false);
        ExtractionConfigRegister.set(builder.buildConfiguration());
        
        ContentExtractor extractor = interruptionTimeoutSecs != null ? new ContentExtractor(interruptionTimeoutSecs)
                : new ContentExtractor();
        extractor.setPDF(contentStream);
        handleContentWithCermine(extractor, documentId);
    }
    
    /**
     * Extracts metadata and plaintext from content using extractor. Writes data to namedOutputMeta.
     * 
     * @param extractor content extractor holding PDF stream
     * @param documentId document identifier
     */
    private void handleContentWithCermine(ContentExtractor extractor, String documentId) throws TimeoutException, AnalysisException, IOException, InterruptedException, TransformationException {
        Element resultElem = extractor.getContentAsNLM();
        Document doc = new Document(resultElem);
        String text = null;
        try {
            text = extractor.getRawFullText();
        } catch (AnalysisException e) {
            log.error("unable to extract plaintext, writing extracted metadata only", e);
        }
        mos.write(namedOutputMeta, new AvroKey<ExtractedDocumentMetadata>(
                NlmToDocumentWithBasicMetadataConverter.convertFull(documentId, doc, text, EXTRACTED_METADATA_RECORD_ORIGIN_CERMINE)));
    }
    
    /**
     * Handles exception by converting it to {@link Fault} and writing it to fault output.
     * Empty {@link ExtractedDocumentMetadata} result is written to metadata output.
     * 
     * @param e Exception to be handled
     * @param documentId document identifier
     * @param extractedBy module name responsible for metadata extraction
     */
    private void handleException(Exception e, String documentId, String extractedBy) throws IOException, InterruptedException {
        // writing empty result
        mos.write(namedOutputMeta, new AvroKey<ExtractedDocumentMetadata>(NlmToDocumentWithBasicMetadataConverter.createEmpty(documentId, extractedBy)));
        // writing fault result
        mos.write(namedOutputFault, new AvroKey<Fault>(FaultUtils.exceptionToFault(documentId, e, null)));
    }
    
    /**
     * Handles document processing time by writing fault when processing time exceeded predefined threshold.
     * @param processingTime processing time in milliseconds
     * @param documentId document identifier
     */
    private void handleProcessingTime(long processingTime, String documentId) throws IOException, InterruptedException {
        if (processingTime > processingTimeThreshold) {
            Map<CharSequence, CharSequence> supplementaryData = new HashMap<CharSequence, CharSequence>();
            supplementaryData.put(FAULT_SUPPLEMENTARY_DATA_PROCESSING_TIME, String.valueOf(processingTime));
            // writing fault result
            mos.write(namedOutputFault,
                    new AvroKey<Fault>(Fault.newBuilder().setInputObjectId(documentId)
                            .setTimestamp(System.currentTimeMillis())
                            .setCode(FAULT_CODE_PROCESSING_TIME_THRESHOLD_EXCEEDED)
                            .setSupplementaryData(supplementaryData).build()));
        }
        log.info("finished processing for id " + documentId + " in " + (processingTime / 1000) + " secs");
    }
    
}
