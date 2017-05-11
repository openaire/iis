package eu.dnetlib.iis.wf.collapsers;

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
    
    public static final String BLOCKING_FIELD = "blocking_field";
    
    private String blockingField;
    
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {      
        blockingField = context.getConfiguration().get(BLOCKING_FIELD);
    }

	@Override
	protected void map(AvroKey<IndexedRecord> key, NullWritable ignore, Context context) throws IOException, InterruptedException {
        String id = (String) CollapserUtils.getNestedFieldValue(key.datum(), blockingField);
        
        context.write(
	        		new AvroKey<String>(id), 
	        		new AvroValue<IndexedRecord>(key.datum()));
	}
    
}
