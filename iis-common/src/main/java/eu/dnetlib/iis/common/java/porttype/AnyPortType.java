package eu.dnetlib.iis.common.java.porttype;

/**
 * A port type that accepts any type of data
 * @author Mateusz Kobos
 *
 */
public class AnyPortType implements PortType {

	@Override
	public String getName() {
		return "Any";
	}

	@Override
	public boolean accepts(PortType other) {
		return true;
	}

}
