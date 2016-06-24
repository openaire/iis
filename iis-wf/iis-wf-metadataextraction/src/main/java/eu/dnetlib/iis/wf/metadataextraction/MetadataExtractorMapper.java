package eu.dnetlib.iis.wf.metadataextraction;

import java.io.IOException;
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
			processStream(content.getId(), new ByteBufferInputStream(byteBuffer));	
		} else {
			log.warn("no byte data found for id: " + content.getId());
		}
	}

}