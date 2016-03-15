package eu.dnetlib.iis.wf.importer.output;

/**
 * Filename generator module.
 * @author mhorst
 *
 * @param <T>
 */
public interface FilenameGenerator<T> {

	/**
	 * Generates filename for given source.
	 * @param source
	 * @return generated file name
	 */
	public String generateFileName(T source);
}
