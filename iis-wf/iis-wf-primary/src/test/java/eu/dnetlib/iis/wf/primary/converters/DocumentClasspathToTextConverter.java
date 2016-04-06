package eu.dnetlib.iis.wf.primary.converters;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.wf.primary.schemas.DocumentContentClasspath;

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
		builder.setText(
				IOUtils.toString(
						Thread.currentThread().getContextClassLoader().getResourceAsStream(
								key.datum().getClasspathLocation().toString()), "utf8"));
        context.write(new AvroKey<DocumentText>(builder.build()),	
        		NullWritable.get());
	}
}

