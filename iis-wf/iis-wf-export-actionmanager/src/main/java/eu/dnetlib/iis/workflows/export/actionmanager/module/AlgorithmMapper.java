package eu.dnetlib.iis.workflows.export.actionmanager.module;


/**
 * Algorithm mapper.
 * @author mhorst
 *
 */
public interface AlgorithmMapper<V> {

	/**
	 * @param algorithmName
	 * @return
	 * @throws MappingNotDefinedException when no value defined for given algorithm.
	 */
	V getValue(AlgorithmName algorithmName) throws MappingNotDefinedException; 
}
