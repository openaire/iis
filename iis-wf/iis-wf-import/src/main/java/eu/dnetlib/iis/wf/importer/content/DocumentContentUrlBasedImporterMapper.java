package eu.dnetlib.iis.wf.importer.content;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_CONNECTION_TIMEOUT;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_CONTENT_READ_TIMEOUT;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;
import eu.dnetlib.iis.importer.schemas.DocumentContent;
import eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters;
import eu.dnetlib.iis.wf.importer.content.appover.ComplexContentApprover;
import eu.dnetlib.iis.wf.importer.content.appover.ContentApprover;
import eu.dnetlib.iis.wf.importer.content.appover.PDFHeaderBasedContentApprover;
import eu.dnetlib.iis.wf.importer.content.appover.SizeLimitContentApprover;

/**
 * {@link DocumentContentUrl} based importer producing {@link DocumentContent} output.
 * @author mhorst
 *
 */
public class DocumentContentUrlBasedImporterMapper extends Mapper<AvroKey<DocumentContentUrl>, NullWritable, AvroKey<DocumentContent>, NullWritable> {

	
	private final Logger log = Logger.getLogger(DocumentContentUrlBasedImporterMapper.class);
	
	/**
	 * Content approver module.
	 */
	private ContentApprover contentApprover;
	
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
        String maxFileSizeMBStr = WorkflowRuntimeParameters.getParamValue(
                ImportWorkflowRuntimeParameters.IMPORT_CONTENT_MAX_FILE_SIZE_MB, context.getConfiguration());
        if (maxFileSizeMBStr != null) {
            this.contentApprover = new ComplexContentApprover(
                    new PDFHeaderBasedContentApprover(),
                    new SizeLimitContentApprover(Integer.valueOf(maxFileSizeMBStr)));
		} else {
			this.contentApprover = new PDFHeaderBasedContentApprover();
		}
	}
	
	@Override
	protected void map(AvroKey<DocumentContentUrl> key, NullWritable value,
			Context context) throws IOException, InterruptedException {
		DocumentContentUrl docUrl = key.datum();
		long startTimeContent = System.currentTimeMillis();
		log.info("starting content retrieval for id: " + docUrl.getId() + 
                ", location: " + docUrl.getUrl() + " and size [kB]: " + docUrl.getContentSizeKB());
		byte[] content = ObjectStoreContentProviderUtils.getContentFromURL(
				docUrl.getUrl().toString(), 
				this.connectionTimeout, this.readTimeout);
		log.info("content retrieval for id: " + docUrl.getId() + " took: " +
				(System.currentTimeMillis()-startTimeContent) + " ms, got content: " +
				(content!=null && content.length>0));
		if (contentApprover.approve(content)) {
			DocumentContent.Builder documentContentBuilder = DocumentContent.newBuilder();
			documentContentBuilder.setId(docUrl.getId());
			if (content!=null) {
				documentContentBuilder.setPdf(ByteBuffer.wrap(
						content));
			}
			context.write(
					new AvroKey<DocumentContent>(documentContentBuilder.build()), 
					NullWritable.get());
		} else {
			log.info("content " + docUrl.getId() + " not approved " +
					"for location: " + docUrl.getUrl() + " and size [kB]: " + docUrl.getContentSizeKB());
		}
		
	}
}
