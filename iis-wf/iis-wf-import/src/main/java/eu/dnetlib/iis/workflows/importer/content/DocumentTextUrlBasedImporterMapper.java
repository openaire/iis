package eu.dnetlib.iis.workflows.importer.content;

import static eu.dnetlib.iis.workflows.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_CONNECTION_TIMEOUT;
import static eu.dnetlib.iis.workflows.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_READ_TIMEOUT;

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

	public static final String IMPORT_CONTENT_MAX_FILE_SIZE_MB = "import.content.max.file.size.mb";
	
	private final Logger log = Logger.getLogger(DocumentContentUrlBasedImporterMapper.class);
	
	/**
     * Maximum content size in kilobytes.
     */
    protected long maxFileSizeKB = Long.MAX_VALUE;
	
	/**
	 * Connection timeout.
	 */
	private int connectionTimeout;
	
	/**
	 * Read timeout.
	 */
	private int readTimeout;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
//		connection and approver related parameters
		this.connectionTimeout = context.getConfiguration().getInt(
									IMPORT_CONTENT_CONNECTION_TIMEOUT, 60000);
		this.readTimeout = context.getConfiguration().getInt(
									IMPORT_CONTENT_READ_TIMEOUT, 60000);
		// handling maximum content size
        String maxFileSizeMBStr = context.getConfiguration().get(IMPORT_CONTENT_MAX_FILE_SIZE_MB);
        if (maxFileSizeMBStr != null && !maxFileSizeMBStr.trim().isEmpty()
                && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(maxFileSizeMBStr)) {
            this.maxFileSizeKB = 1024l * Integer.valueOf(maxFileSizeMBStr);
        }
	}
	
	@Override
	protected void map(AvroKey<DocumentContentUrl> key, NullWritable value,
			Context context) throws IOException, InterruptedException {
		DocumentContentUrl docUrl = key.datum();
		if (docUrl.getContentSizeKB() > maxFileSizeKB) {
            log.warn("skipping processing for id " + docUrl.getId() 
            		+ " due to max file size limit=" + maxFileSizeKB
                    + " KB exceeded: " + docUrl.getContentSizeKB() + " KB");
		} else {
			long startTimeContent = System.currentTimeMillis();
			byte[] textContent = ObjectStoreContentProviderUtils.getContentFromURL(
					docUrl.getUrl().toString(), connectionTimeout, readTimeout);
			log.info("text content retrieval for id: " + docUrl.getId() + 
					" and location: " + docUrl.getUrl() + " took: " +
					(System.currentTimeMillis()-startTimeContent) + " ms, got text content: " +
					(textContent!=null && textContent.length>0));
			DocumentText.Builder documentTextBuilder = DocumentText.newBuilder();
			documentTextBuilder.setId(docUrl.getId());
			if (textContent!=null) {
				documentTextBuilder.setText(new String(textContent, 
						ObjectStoreContentProviderUtils.defaultEncoding));
			}
			context.write(
					new AvroKey<DocumentText>(documentTextBuilder.build()), 
					NullWritable.get());
		}
	}

}
