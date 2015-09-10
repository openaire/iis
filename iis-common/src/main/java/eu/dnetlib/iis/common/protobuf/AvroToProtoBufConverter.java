package eu.dnetlib.iis.common.protobuf;

import com.google.protobuf.Message;
import org.apache.avro.generic.IndexedRecord;

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
public interface AvroToProtoBufConverter<IN extends IndexedRecord, OUT extends Message> {
    public String convertIntoKey(IN datum);
    public OUT convertIntoValue(IN datum);
}
