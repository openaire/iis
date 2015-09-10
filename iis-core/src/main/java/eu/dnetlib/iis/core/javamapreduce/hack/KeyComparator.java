package eu.dnetlib.iis.core.javamapreduce.hack;

import org.apache.avro.hadoop.io.AvroKeyComparator;
import org.apache.hadoop.conf.Configuration;

/**
 * Class to be used in Oozie map-reduce workflow node definition
 * @author Mateusz Kobos
 */
public class KeyComparator<T> extends AvroKeyComparator<T> {
	@Override
	public void setConf(Configuration conf) {
		SchemaSetter.set(conf);
		super.setConf(conf);
	}
}
