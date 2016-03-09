package eu.dnetlib.iis.workflows.importer;

import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.fs.Path;

/**
 * Object with path tuple.
 * @author mhorst
 *
 */
public class ObjectWithPath {
	final private SpecificRecord object;
	final private Path path;
	
	public ObjectWithPath(SpecificRecord object, Path path) {
		this.object = object;
		this.path = path;
	}

	public SpecificRecord getObject() {
		return object;
	}

	public Path getPath() {
		return path;
	}
}
