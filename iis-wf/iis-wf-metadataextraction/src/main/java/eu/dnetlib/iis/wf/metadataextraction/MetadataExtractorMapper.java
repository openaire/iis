package eu.dnetlib.iis.wf.metadataextraction;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.zookeeper.server.ByteBufferInputStream;

import eu.dnetlib.iis.importer.schemas.DocumentContent;

/**
 * Metadata extractor module.
 * @author Mateusz Kobos
 * @author mhorst
 *
 */
public class MetadataExtractorMapper extends AbstractMetadataExtractorMapper<DocumentContent> {
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(AvroKey<DocumentContent> key, NullWritable ignore, Context context)
			throws IOException, InterruptedException {
		DocumentContent content = key.datum();
		if (content.getPdf()!=null) {
			ByteBuffer byteBuffer = content.getPdf();
			try (InputStream inputStream = new ByteBufferInputStream(byteBuffer)) {
			    processStream(content.getId().toString(), inputStream);
			}
				
		} else {
			log.warn("no byte data found for id: " + content.getId());
		}
	}

}