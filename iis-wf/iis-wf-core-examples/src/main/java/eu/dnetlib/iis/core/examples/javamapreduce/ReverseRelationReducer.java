package eu.dnetlib.iis.core.examples.javamapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.Document;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.PersonWithDocuments;

/**
 * @author Mateusz Kobos
 */
public class ReverseRelationReducer
		extends	Reducer<AvroKey<Integer>, AvroValue<Document>, 
			AvroKey<PersonWithDocuments>, NullWritable> {

	@Override
	public void reduce(AvroKey<Integer> key,
			Iterable<AvroValue<Document>> values, Context context)
			throws IOException, InterruptedException {

		ArrayList<Document> documents = new ArrayList<Document>();
		for(AvroValue<Document> doc: values){
			/** We need to do a copy because it seems that the same "doc" 
			 * object is reused in consecutive iteration steps. Thus if we
			 * had not done the copy, all elements in the list would have 
			 * pointed to the same object.  
			 */
			Document copy = new Document(doc.datum().getId(), 
					doc.datum().getTitle());
			documents.add(copy);
		}
		PersonWithDocuments person = new PersonWithDocuments(
				key.datum(), documents);
		context.write(new AvroKey<PersonWithDocuments>(person), 
				NullWritable.get());
	}
}
