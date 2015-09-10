package eu.dnetlib.iis.mainworkflows.converters;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.mapred.AvroKey;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import eu.dnetlib.iis.importer.schemas.DocumentContent;
import eu.dnetlib.iis.mainworkflows.schemas.DocumentContentClasspath;

/**
 * Mapper converting {@link DocumentContentClasspath} to {@link DocumentContent} 
 * by retrieving content from classpath.
 * 
 * @author mhorst
 */
public class DocumentClasspathToContentConverter 
extends Mapper<AvroKey<DocumentContentClasspath>, NullWritable, AvroKey<DocumentContent>, NullWritable> {
	
	@Override
	protected void map(AvroKey<DocumentContentClasspath> key, NullWritable ignore, Context context)
					 throws IOException, InterruptedException{
		DocumentContent.Builder builder = DocumentContent.newBuilder();
		builder.setId(key.datum().getId());
		builder.setPdf(ByteBuffer.wrap(
				IOUtils.toByteArray(
						Thread.currentThread().getContextClassLoader().getResourceAsStream(
								key.datum().getClasspathLocation().toString()))));
        context.write(new AvroKey<DocumentContent>(builder.build()),	
        		NullWritable.get());
	}
}

