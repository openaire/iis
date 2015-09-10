package eu.dnetlib.iis.core.javamapreduce.hack;

import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Dominika Tkaczyk
 */
public class Serialization<T> extends AvroSerialization<T> {

    @Override
    public void setConf(Configuration conf) {
        SchemaSetter.set(conf);
        super.setConf(conf);
    }
    
}
