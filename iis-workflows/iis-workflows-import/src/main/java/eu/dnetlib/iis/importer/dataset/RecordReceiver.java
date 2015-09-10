package eu.dnetlib.iis.importer.dataset;

import java.io.IOException;

/**
 * Record receiver interface.
 * @author mhorst
 *
 * @param <T>
 */
public interface RecordReceiver<T> {

	public void receive(T object) throws IOException; 
}
