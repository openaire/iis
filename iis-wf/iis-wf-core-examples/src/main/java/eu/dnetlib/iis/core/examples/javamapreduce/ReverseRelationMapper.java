package eu.dnetlib.iis.core.examples.javamapreduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.Document;

/**
 * @author Mateusz Kobos
 */
public class ReverseRelationMapper 
extends Mapper<AvroKey<eu.dnetlib.iis.core.examples.schemas.documentandauthor.Document>, 
	NullWritable, AvroKey<Integer>, AvroValue<Document>> {
	
	@Override
	protected void map(AvroKey<eu.dnetlib.iis.core.examples.schemas.documentandauthor.Document> key, 
			NullWritable ignore, Context context) throws IOException, InterruptedException{
		Document documentInfo =	new Document(
				key.datum().getId(), key.datum().getTitle());
		
		for(int authorId: key.datum().getAuthorIds()){
			context.write(new AvroKey<Integer>(authorId), 
					new AvroValue<Document>(documentInfo));
		}
	}
}
