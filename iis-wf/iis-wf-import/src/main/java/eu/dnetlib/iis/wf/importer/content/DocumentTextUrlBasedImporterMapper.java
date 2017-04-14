package eu.dnetlib.iis.wf.importer.content;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_CONNECTION_TIMEOUT;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_MAX_FILE_SIZE_MB;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_READ_TIMEOUT;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;

/**
 * {@link DocumentContentUrl} based importer producing {@link DocumentText} output.
 * @author mhorst
 *
 */
public class DocumentTextUrlBasedImporterMapper extends Mapper<AvroKey<DocumentContentUrl>, NullWritable, AvroKey<DocumentText>, NullWritable> {


    private static final Logger log = Logger.getLogger(DocumentContentUrlBasedImporterMapper.class);

    /**
     * Maximum content size in kilobytes.
     */
    private long maxFileSizeKB = Long.MAX_VALUE;

    /**
     * Connection timeout.
     */
    private int connectionTimeout;

    /**
     * Read timeout.
     */
    private int readTimeout;


    /**
     * Hadoop counters enum of invalid records 
     */
    public static enum InvalidRecordCounters {
        SIZE_EXCEEDED,
        SIZE_INVALID
    }


    @Override
    protected void setup(Context context) {
        // connection related parameters
        this.connectionTimeout = context.getConfiguration().getInt(
                IMPORT_CONTENT_CONNECTION_TIMEOUT, 60000);
        this.readTimeout = context.getConfiguration().getInt(
                IMPORT_CONTENT_READ_TIMEOUT, 60000);
        // handling maximum content size
        Integer maxFileSizeMB = WorkflowRuntimeParameters.getIntegerParamValue(
                IMPORT_CONTENT_MAX_FILE_SIZE_MB, context.getConfiguration());
        if (maxFileSizeMB != null) {
            this.maxFileSizeKB = 1024l * maxFileSizeMB;
        }
        
        context.getCounter(InvalidRecordCounters.SIZE_EXCEEDED).setValue(0);
        context.getCounter(InvalidRecordCounters.SIZE_INVALID).setValue(0);
    }
    
    /**
     * Provides contents for given url.
     */
    protected byte[] getContent(String url) throws IOException, InvalidSizeException {
        return ObjectStoreContentProviderUtils.getContentFromURL(
                url, this.connectionTimeout, this.readTimeout);
    }
    
    @Override
    protected void map(AvroKey<DocumentContentUrl> key, NullWritable value,
            Context context) throws IOException, InterruptedException {
        DocumentContentUrl docUrl = key.datum();
        if (docUrl.getContentSizeKB() <= 0) {
            log.warn("content " + docUrl.getId() + " discarded for location: " + docUrl.getUrl()
            + " and size [kB]: " + docUrl.getContentSizeKB() + ", size is expected to be greater than 0!");
            context.getCounter(InvalidRecordCounters.SIZE_INVALID).increment(1);
        } else if (docUrl.getContentSizeKB() <= maxFileSizeKB) {
            try {
                long startTimeContent = System.currentTimeMillis();
                byte[] textContent = getContent(docUrl.getUrl().toString());
                log.info("text content retrieval for id: " + docUrl.getId() + 
                        " and location: " + docUrl.getUrl() + " took: " +
                        (System.currentTimeMillis()-startTimeContent) + " ms");
                DocumentText.Builder documentTextBuilder = DocumentText.newBuilder();
                documentTextBuilder.setId(docUrl.getId());
                documentTextBuilder.setText(new String(textContent, 
                        ObjectStoreContentProviderUtils.defaultEncoding));
                context.write(new AvroKey<DocumentText>(documentTextBuilder.build()), NullWritable.get());            
                
            } catch (InvalidSizeException e) {
                log.warn("content " + docUrl.getId() + " discarded for location: " + docUrl.getUrl()
                + ", real size is expected to be greater than 0!");
                context.getCounter(InvalidRecordCounters.SIZE_INVALID).increment(1);
            }
        } else {
            context.getCounter(InvalidRecordCounters.SIZE_EXCEEDED).increment(1);
            log.warn("skipping processing for id " + docUrl.getId() 
                    + " due to max file size limit=" + maxFileSizeKB
                    + " KB exceeded: " + docUrl.getContentSizeKB() + " KB");
        }
    }
}
