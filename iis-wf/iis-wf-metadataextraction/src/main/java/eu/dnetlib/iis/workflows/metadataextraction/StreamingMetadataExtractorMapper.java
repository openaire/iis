package eu.dnetlib.iis.workflows.metadataextraction;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;

/**
 * Streaming version of metadata extractor.
 * Reads document content directly from URL stream.
 * @author mhorst
 *
 */
public class StreamingMetadataExtractorMapper extends AbstractMetadataExtractorMapper<DocumentContentUrl> {
	
	public static final String IMPORT_CONTENT_CONNECTION_TIMEOUT = "import.content.connection.timeout";
	public static final String IMPORT_CONTENT_READ_TIMEOUT = "import.content.read.timeout";
	
	protected int connectionTimeout;
	
	protected int readTimeout;
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		this.connectionTimeout = context.getConfiguration().getInt(
				IMPORT_CONTENT_CONNECTION_TIMEOUT, 60000);
		this.readTimeout = context.getConfiguration().getInt(
				IMPORT_CONTENT_READ_TIMEOUT, 60000);
		super.setup(context);
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(AvroKey<DocumentContentUrl> key, NullWritable ignore, Context context)
			throws IOException, InterruptedException {
		DocumentContentUrl contentUrl = key.datum();
		URL url = new URL(contentUrl.getUrl().toString());
		URLConnection con = url.openConnection();
		con.setConnectTimeout(this.connectionTimeout);
		con.setReadTimeout(this.readTimeout);
		Map<CharSequence, CharSequence> auditSupplementaryData = new HashMap<CharSequence, CharSequence>();
		auditSupplementaryData.put(FAULT_SUPPLEMENTARY_DATA_URL, contentUrl.getUrl());
		processStream(contentUrl.getId(), con.getInputStream(), contentUrl.getContentSizeKB(), auditSupplementaryData);
	}
	
}
