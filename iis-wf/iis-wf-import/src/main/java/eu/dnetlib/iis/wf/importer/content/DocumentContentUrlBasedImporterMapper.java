package eu.dnetlib.iis.wf.importer.content;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_CONNECTION_TIMEOUT;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_MAX_FILE_SIZE_MB;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_READ_TIMEOUT;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;
import eu.dnetlib.iis.importer.schemas.DocumentContent;

/**
 * {@link DocumentContentUrl} based importer producing {@link DocumentContent} output.
 * 
 * @author mhorst
 *
 */
public class DocumentContentUrlBasedImporterMapper
        extends Mapper<AvroKey<DocumentContentUrl>, NullWritable, AvroKey<DocumentContent>, NullWritable> {

    private static final Logger log = Logger.getLogger(DocumentContentUrlBasedImporterMapper.class);


    /**
     * Connection timeout.
     */
    private int connectionTimeout;

    /**
     * Read timeout.
     */
    private int readTimeout;
    
    /**
     * Maximum allowed file size expressed in KB.
     */
    private long maxFileSizeKB = Long.MAX_VALUE;
    
    /**
     * Counter for the records with content size exceeded.
     */
    private Counter sizeExceededCounter;
    
    /**
     * Counter for the records with invalid size: less or equal 0.
     */
    private Counter sizeInvalidCounter;
    
    /**
     * Hadoop counters enum of invalid records 
     */
    public static enum InvalidRecordCounters {
        SIZE_EXCEEDED,
        SIZE_INVALID
    }
    
    //------------------------ LOGIC --------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // connection and approver related parameters
        this.connectionTimeout = context.getConfiguration().getInt(IMPORT_CONTENT_CONNECTION_TIMEOUT, 60000);
        this.readTimeout = context.getConfiguration().getInt(IMPORT_CONTENT_READ_TIMEOUT, 60000);
        
        this.sizeInvalidCounter = context.getCounter(InvalidRecordCounters.SIZE_INVALID);
        this.sizeInvalidCounter.setValue(0);
        
        this.sizeExceededCounter = context.getCounter(InvalidRecordCounters.SIZE_EXCEEDED);
        this.sizeExceededCounter.setValue(0);
        Integer maxFileSizeMB = WorkflowRuntimeParameters.getIntegerParamValue(
                IMPORT_CONTENT_MAX_FILE_SIZE_MB, context.getConfiguration());
        if (maxFileSizeMB != null) {
            this.maxFileSizeKB = 1024l * maxFileSizeMB;
        }
    }

    @Override
    protected void map(AvroKey<DocumentContentUrl> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
        DocumentContentUrl docUrl = key.datum();
        if (docUrl.getContentSizeKB() <= 0) {
            log.warn("content " + docUrl.getId() + " discarded for location: " + docUrl.getUrl()
            + " and size [kB]: " + docUrl.getContentSizeKB() + ", size is expected to be greater than 0!");
            this.sizeInvalidCounter.increment(1);
        } else if (docUrl.getContentSizeKB() <= maxFileSizeKB) {
            long startTimeContent = System.currentTimeMillis();
            log.info("starting content retrieval for id: " + docUrl.getId() + ", location: " + docUrl.getUrl()
                    + " and size [kB]: " + docUrl.getContentSizeKB());
            try {
                byte[] content = ObjectStoreContentProviderUtils.getContentFromURL(docUrl.getUrl().toString(),
                        this.connectionTimeout, this.readTimeout);
                DocumentContent.Builder documentContentBuilder = DocumentContent.newBuilder();
                documentContentBuilder.setId(docUrl.getId());
                documentContentBuilder.setPdf(ByteBuffer.wrap(content));
                context.write(new AvroKey<DocumentContent>(documentContentBuilder.build()), NullWritable.get());
                log.info("content retrieval for id: " + docUrl.getId() + " took: "
                        + (System.currentTimeMillis() - startTimeContent) + " ms");
            
            } catch (InvalidSizeException e) {
                log.warn("content " + docUrl.getId() + " discarded for location: " + docUrl.getUrl()
                + ", real size is expected to be greater than 0!");
                this.sizeInvalidCounter.increment(1);
            }
        } else {
            this.sizeExceededCounter.increment(1);
            log.info("content " + docUrl.getId() + " discarded for location: " + docUrl.getUrl()
            + " and size [kB]: " + docUrl.getContentSizeKB() + ", size limit: " + maxFileSizeKB + " exceeded!");
        }

    }
}
