package eu.dnetlib.iis.common.model.extrainfo.citations;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;

/**
 * Typed identifier.
 * @author mhorst
 *
 */
@XStreamAlias("id")
public class TypedId {
	
	@XStreamAsAttribute
	String value;
	@XStreamAsAttribute
	String type;
	@XStreamAsAttribute
	float confidenceLevel;
	
	public TypedId(String value, String type,
			float confidenceLevel) {
		this.value = value;
		this.type = type;
		this.confidenceLevel = confidenceLevel;
	}
	
	public TypedId() {
		super();
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public float getConfidenceLevel() {
		return confidenceLevel;
	}

	public void setConfidenceLevel(float confidenceLevel) {
		this.confidenceLevel = confidenceLevel;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Float.floatToIntBits(confidenceLevel);
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TypedId other = (TypedId) obj;
		if (Float.floatToIntBits(confidenceLevel) != Float
				.floatToIntBits(other.confidenceLevel))
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
}