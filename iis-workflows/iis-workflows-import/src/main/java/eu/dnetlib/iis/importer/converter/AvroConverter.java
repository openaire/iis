package eu.dnetlib.iis.importer.converter;

import org.apache.hadoop.hbase.client.Result;

import eu.dnetlib.data.proto.OafProtos.Oaf;

/**
 * HBase {@link Result} to avro object converter.
 * @author mhorst
 *
 * @param <T>
 */
public interface AvroConverter<T> {

	/**
	 * Builds object for given parameters.
	 * @param hbaseResult
	 * @param resolvedOafObject
	 * @return avro object
	 * @throws Exception
	 */
	public T buildObject(Result hbaseResult, Oaf resolvedOafObject) throws Exception;
}
