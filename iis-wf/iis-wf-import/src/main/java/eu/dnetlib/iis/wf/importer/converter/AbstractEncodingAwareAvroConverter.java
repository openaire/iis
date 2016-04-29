package eu.dnetlib.iis.wf.importer.converter;

import eu.dnetlib.iis.wf.importer.input.approver.ResultApprover;

/**
 * Encoding aware avro converter.
 * @author mhorst
 *
 * @param <T>
 */
public abstract class AbstractEncodingAwareAvroConverter<T> extends AbstractAvroConverter<T> {

	/**
	 * Data encoding.
	 */
	private final String encoding;
	
	// ------------------------ CONSTRUCTORS --------------------------
	
	/**
	 * @param encoding encoding to be used when building {@link String} from byte[]
	 * @param resultApprover result approver
	 */
	public AbstractEncodingAwareAvroConverter(String encoding,
			ResultApprover resultApprover) {
		super(resultApprover);
		this.encoding = encoding;
	}
	
	// ------------------------ GETTERS --------------------------
	
	/**
	 * Return encoding to be used when building {@link String} from byte[]
	 */
	public String getEncoding() {
		return encoding;
	}
	
}
