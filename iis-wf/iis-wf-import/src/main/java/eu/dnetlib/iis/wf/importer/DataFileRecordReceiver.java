package eu.dnetlib.iis.wf.importer;

import java.io.IOException;

import org.apache.avro.file.DataFileWriter;

/**
 * {@link DataFileWriter} based record receiver.
 * @author mhorst
 *
 */
public class DataFileRecordReceiver<T> implements RecordReceiver<T> {

	private final DataFileWriter<T> writer;
	
	/**
	 * Default constructor.
	 * @param writer
	 */
	public DataFileRecordReceiver(DataFileWriter<T> writer) {
		this.writer = writer;
	}
	
	@Override
	public void receive(T object) throws IOException {
		this.writer.append(object);
	}

}
