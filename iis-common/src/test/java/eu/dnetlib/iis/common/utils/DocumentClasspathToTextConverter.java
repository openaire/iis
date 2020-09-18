package eu.dnetlib.iis.common.utils;

import java.io.IOException;

import eu.dnetlib.iis.common.StaticResourceProvider;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.common.schemas.DocumentContentClasspath;

/**
 * Mapper converting {@link DocumentContentClasspath} to {@link DocumentText} 
 * by retrieving text content from classpath location.
 * 
 * @author mhorst
 */
public class DocumentClasspathToTextConverter 
extends Mapper<AvroKey<DocumentContentClasspath>, NullWritable, AvroKey<DocumentText>, NullWritable> {
	
	@Override
	protected void map(AvroKey<DocumentContentClasspath> key, NullWritable ignore, Context context)
					 throws IOException, InterruptedException{
		DocumentText.Builder builder = DocumentText.newBuilder();
		builder.setId(key.datum().getId());
		builder.setText(StaticResourceProvider.getResourceContent(key.datum().getClasspathLocation()));
        context.write(new AvroKey<DocumentText>(builder.build()),	
        		NullWritable.get());
	}
}

