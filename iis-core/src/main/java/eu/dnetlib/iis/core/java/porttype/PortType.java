package eu.dnetlib.iis.core.java.porttype;

/**
 * Type of the port. This is used to specify what kind of data is 
 * accepted on a certain input port or produced on a certain output port 
 * of a workflow node.
 * 
 * @author Mateusz Kobos
 *
 */
public interface PortType {
	
	String getName();
	
	/**
	 * This should be used to check whether data produced by a workflow node
	 * conforms to the data consumed by other workflow node. 
	 * In a scenario when A produces certain data on a port p and B consumes 
	 * this data on a port q, type(q).accepts(type(p)) has to be true.
	 * 
	 * @return {@code true} if {@code this} port type is a more general 
	 * version of the {@code other} port type, 
	 * or as an alternative explanation: {@code other} is a subset of 
	 * {@code this}, i.e. {@code other} has at least all the properties present 
	 * in {@code this} (and possibly some others). This is analogous to a 
	 * situation in object-oriented programming, where in order for assignment 
	 * operation {@code this = other} to work, the type of {@code this} has to 
	 * accept type of {@code other}, or in other words {@code other} has to 
	 * inherit from {@code this}, or in yet other words: {@code other} has to
	 * conform to {@code this}.
	 */
	boolean accepts(PortType other);
}
