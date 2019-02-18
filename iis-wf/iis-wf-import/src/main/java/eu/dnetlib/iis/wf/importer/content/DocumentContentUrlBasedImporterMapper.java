package eu.dnetlib.iis.wf.importer.content;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

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
     * Content retrieval runtime context.
     */
    private ContentRetrievalContext contentRetrievalContext;
    
    /**
     * Counter for the records with content size exceeded.
     */
    private Counter sizeExceededCounter;
    
    /**
     * Counter for the records with invalid size: less or equal 0.
     */
    private Counter sizeInvalidCounter;
    
    /**
     * Counter for the unavailable documents.
     */
    private Counter unavailableCounter;
    
    /**
     * Hadoop counters enum of invalid records 
     */
    public static enum InvalidRecordCounters {
        SIZE_EXCEEDED,
        SIZE_INVALID,
        UNAVAILABLE
    }
    
    //------------------------ LOGIC --------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // connection and approver related parameters
        this.contentRetrievalContext = new ContentRetrievalContext(context.getConfiguration());
        
        this.sizeInvalidCounter = context.getCounter(InvalidRecordCounters.SIZE_INVALID);
        this.sizeInvalidCounter.setValue(0);
        
        this.sizeExceededCounter = context.getCounter(InvalidRecordCounters.SIZE_EXCEEDED);
        this.sizeExceededCounter.setValue(0);

        this.unavailableCounter = context.getCounter(InvalidRecordCounters.UNAVAILABLE);
        this.unavailableCounter.setValue(0);
    }

    /**
     * Provides contents for given url.
     */
    protected byte[] getContent(String url) throws IOException, InvalidSizeException, S3EndpointNotFoundException {
        return ObjectStoreContentProviderUtils.getContentFromURL(url, this.contentRetrievalContext);
    }
    
    @Override
    protected void map(AvroKey<DocumentContentUrl> key, NullWritable value, Context context)
            throws IOException, InterruptedException {
        DocumentContentUrl docUrl = key.datum();
        if (docUrl.getContentSizeKB() <= 0) {
            log.warn("content " + docUrl.getId() + " discarded for location: " + docUrl.getUrl()
            + " and size [kB]: " + docUrl.getContentSizeKB() + ", size is expected to be greater than 0!");
            this.sizeInvalidCounter.increment(1);
        } else if (docUrl.getContentSizeKB() <= this.contentRetrievalContext.getMaxFileSizeKB()) {
            long startTimeContent = System.currentTimeMillis();
            log.info("starting content retrieval for id: " + docUrl.getId() + ", location: " + docUrl.getUrl()
                    + " and size [kB]: " + docUrl.getContentSizeKB());
            try {
                byte[] content = getContent(docUrl.getUrl().toString());
                DocumentContent.Builder documentContentBuilder = DocumentContent.newBuilder();
                documentContentBuilder.setId(docUrl.getId());
                documentContentBuilder.setPdf(ByteBuffer.wrap(content));
                context.write(new AvroKey<DocumentContent>(documentContentBuilder.build()), NullWritable.get());
                log.info("content retrieval for id: " + docUrl.getId() + " took: "
                        + (System.currentTimeMillis() - startTimeContent) + " ms");
            
            } catch (S3EndpointNotFoundException e) {
                throw new IOException("Got S3 link: " + docUrl.getUrl() + " but no S3 endpoint was specified in job configuration!", e);
            } catch (InvalidSizeException e) {
                log.warn("content " + docUrl.getId() + " discarded for location: " + docUrl.getUrl()
                + ", real size is expected to be greater than 0!");
                this.sizeInvalidCounter.increment(1);
            } catch (Exception e) {
                log.error("unexpected exception occured while obtaining content " + docUrl.getId() 
                + " for location: " + docUrl.getUrl(), e);
                this.unavailableCounter.increment(1);
            }
        } else {
            this.sizeExceededCounter.increment(1);
            log.info("content " + docUrl.getId() + " discarded for location: " + docUrl.getUrl()
            + " and size [kB]: " + docUrl.getContentSizeKB() + ", size limit: " + this.contentRetrievalContext.getMaxFileSizeKB() + " exceeded!");
        }

    }
}
