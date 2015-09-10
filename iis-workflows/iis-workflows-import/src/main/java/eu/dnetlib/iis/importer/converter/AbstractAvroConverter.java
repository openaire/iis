package eu.dnetlib.iis.importer.converter;

import org.apache.hadoop.hbase.client.Result;

import eu.dnetlib.iis.importer.input.approver.ResultApprover;

/**
 * Abstract Hbase {@link Result} to avro object converter.
 * @author mhorst
 *
 * @param <T>
 */
public abstract class AbstractAvroConverter<T> implements AvroConverter<T> {
	
	/**
	 * Data encoding.
	 */
	private final String encoding;
	
	/**
	 * Result approver.
	 */
	protected final ResultApprover resultApprover;
	
	
	/**
	 * Default constructor.
	 * @param encoding
	 * @param resultApprover
	 */
	public AbstractAvroConverter(String encoding,
			ResultApprover resultApprover) {
		this.encoding = encoding;
		this.resultApprover = resultApprover;
	}
	

	public String getEncoding() {
		return encoding;
	}
	
}
