package eu.dnetlib.iis.workflows.citationmatching;

import eu.dnetlib.iis.citationmatching.schemas.Citation;
import eu.dnetlib.iis.citationmatching.schemas.PartialCitation;
import eu.dnetlib.iis.workflows.citationmatching.converter.entity_id.CitEntityId;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
public class PartialCitationMerger extends Reducer<AvroKey<String>, AvroValue<PartialCitation>, AvroKey<Citation>, NullWritable> {
    @Override
    protected void reduce(AvroKey<String> key, Iterable<AvroValue<PartialCitation>> values, Context context) throws IOException, InterruptedException {
        CitEntityId entityId = CitEntityId.parseFrom(key.toString());

        Citation result = new Citation();
        result.setSourceDocumentId(entityId.getSourceDocumentId());
        result.setPosition(entityId.getPosition());
        for (AvroValue<PartialCitation> value : values) {
            PartialCitation cit = value.datum();
            if (cit.getDestinationDocumentId() != null) {
                result.setDestinationDocumentId(cit.getDestinationDocumentId());
                result.setConfidenceLevel(cit.getConfidenceLevel());
            }
        }
        
       	context.write(new AvroKey<Citation>(result), NullWritable.get());	
    }
}
