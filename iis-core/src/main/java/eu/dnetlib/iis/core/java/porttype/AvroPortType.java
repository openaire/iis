package eu.dnetlib.iis.core.java.porttype;

import org.apache.avro.Schema;
import org.apache.commons.lang.NotImplementedException;

/**
 * This port type accepts data stores in a format of Avro 
 * Object Container Files, i.e. Avro data files. 
 * This kind of file corresponds to a list of objects, each one being of the
 * same type, i.e. each one is defined by the same Avro schema.
 * @author Mateusz Kobos
 */
public class AvroPortType implements PortType {
	
	private final Schema schema;
	

	public AvroPortType(Schema schema) {
		this.schema = schema;
	}

	@Override
	public String getName() {
		return schema.getFullName();
	}

	@Override
	/** Simple check if the port types are exactly the same 
	 * (as defined by the {@code equals} method). 
	 * 
	 * TODO: this should work in a more relaxed way - 
	 * {@code this.accepts(other)} should be true if {@code this} 
	 * describes a subset of structures defined in {@code other}. To be
	 * more precise: the JSON schema tree tree defined by {@code this} should
	 * form a sub-tree of the JSON schema tree defined by {@code other}. */
	public boolean accepts(PortType other) {
		return this.equals(other);
	}
	
	/**
	 * Two patterns are equal if their schemas are the same.
	 */
	@Override
	public boolean equals(Object o){
		if(!(o instanceof AvroPortType)){
			return false;
		}
		AvroPortType other = (AvroPortType) o;
		return this.schema.equals(other.schema);
	}
	
	@Override
	public int hashCode(){
		throw new NotImplementedException();
	}

	/**
	 * Returns avro schema.
	 * @return avro schema
	 */
	public Schema getSchema() {
		return schema;
	}

}
