package eu.dnetlib.iis.workflows.collapsers;

import java.io.IOException;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Dominika Tkaczyk
 */
public class GroupByFieldMapper extends Mapper<AvroKey<IndexedRecord>, NullWritable, AvroKey<String>, AvroValue<IndexedRecord>> {
    
    private String blockingField;
    
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {      
        blockingField = context.getConfiguration().get("blocking_field");
    }

	@Override
	protected void map(AvroKey<IndexedRecord> key, NullWritable ignore, Context context) throws IOException, InterruptedException {
        String id = (String) CollapserUtils.getNestedFieldValue(key.datum(), blockingField);
        
        context.write(
	        		new AvroKey<String>(id), 
	        		new AvroValue<IndexedRecord>(key.datum()));
	}
    
}
