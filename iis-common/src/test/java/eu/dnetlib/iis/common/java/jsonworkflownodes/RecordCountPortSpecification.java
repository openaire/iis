package eu.dnetlib.iis.common.java.jsonworkflownodes;

import org.apache.avro.Schema;

/**
 * Port specification for test consumer {@link RecordCountTestConsumer}.
 * 
 * @author madryk
 */
public class RecordCountPortSpecification {

	private final String name;
	private final Schema schema;
	private final int minimumRecordCount;
	private final int maximumRecordCount;
	
	
	//------------------------ CONSTRUCTORS --------------------------
	
	public RecordCountPortSpecification(String name, Schema schema, int minimumRecordCount, int maximumRecordCount) {
		this.name = name;
		this.schema = schema;
		this.minimumRecordCount = minimumRecordCount;
		this.maximumRecordCount = maximumRecordCount;
	}


	//------------------------ GETTERS --------------------------
	
	/**
	 * @return name of port
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return schema for records in avro data store
	 */
	public Schema getSchema() {
		return schema;
	}

	/**
	 * @return minimum number of records that can be in avro data store
	 */
	public int getMinimumRecordCount() {
		return minimumRecordCount;
	}

	/**
	 * @return maximum number of records that can be in avro data store
	 */
	public int getMaximumRecordCount() {
		return maximumRecordCount;
	}
}
