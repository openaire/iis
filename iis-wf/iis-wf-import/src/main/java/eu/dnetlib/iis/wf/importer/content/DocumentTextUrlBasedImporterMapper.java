package eu.dnetlib.iis.wf.importer.content;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

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
     * Content retrieval runtime context.
     */
    private ContentRetrievalContext contentRetrievalContext;

    /**
     * Hadoop counters enum of invalid records 
     */
    public static enum InvalidRecordCounters {
        SIZE_EXCEEDED,
        SIZE_INVALID,
        UNAVAILABLE
    }


    @Override
    protected void setup(Context context) {
        // connection related parameters
        this.contentRetrievalContext = new ContentRetrievalContext(context.getConfiguration());
        
        context.getCounter(InvalidRecordCounters.SIZE_EXCEEDED).setValue(0);
        context.getCounter(InvalidRecordCounters.SIZE_INVALID).setValue(0);
        context.getCounter(InvalidRecordCounters.UNAVAILABLE).setValue(0);
    }
    
    /**
     * Provides contents for given url.
     */
    protected byte[] getContent(String url) throws IOException, InvalidSizeException, S3EndpointNotFoundException {
        return ObjectStoreContentProviderUtils.getContentFromURL(url, this.contentRetrievalContext);
    }
    
    @Override
    protected void map(AvroKey<DocumentContentUrl> key, NullWritable value,
            Context context) throws IOException, InterruptedException {
        DocumentContentUrl docUrl = key.datum();
        if (docUrl.getContentSizeKB() <= 0) {
            log.warn("content " + docUrl.getId() + " discarded for location: " + docUrl.getUrl()
            + " and size [kB]: " + docUrl.getContentSizeKB() + ", size is expected to be greater than 0!");
            context.getCounter(InvalidRecordCounters.SIZE_INVALID).increment(1);
        } else if (docUrl.getContentSizeKB() <= this.contentRetrievalContext.getMaxFileSizeKB()) {
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
                
            } catch (S3EndpointNotFoundException e) {
                throw new IOException("Got S3 link: " + docUrl.getUrl() + " but no S3 endpoint was specified in job configuration!", e);
            } catch (InvalidSizeException e) {
                log.warn("content " + docUrl.getId() + " discarded for location: " + docUrl.getUrl()
                + ", real size is expected to be greater than 0!");
                context.getCounter(InvalidRecordCounters.SIZE_INVALID).increment(1);
            } catch (Exception e) {
                log.error("unexpected exception occured while obtaining content " + docUrl.getId() + " for location: "
                        + docUrl.getUrl(), e);
                context.getCounter(InvalidRecordCounters.UNAVAILABLE).increment(1);
            }
        } else {
            context.getCounter(InvalidRecordCounters.SIZE_EXCEEDED).increment(1);
            log.warn("skipping processing for id " + docUrl.getId() 
                    + " due to max file size limit=" + this.contentRetrievalContext.getMaxFileSizeKB()
                    + " KB exceeded: " + docUrl.getContentSizeKB() + " KB");
        }
    }
}
