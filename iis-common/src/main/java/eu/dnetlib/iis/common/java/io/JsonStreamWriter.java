package eu.dnetlib.iis.common.java.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.HackedJsonEncoder;

/**
 * Write JSON stream corresponding a series of Avro datums. Single line in the
 * output corresponds to a single input Avro datum.
 * 
 * @author Mateusz Kobos
 */
public class JsonStreamWriter<T> implements Closeable {
	private final OutputStream out;
	private final HackedJsonEncoder encoder;
	private final GenericDatumWriter<T> writer;

	public JsonStreamWriter(Schema schema, OutputStream out) throws IOException {
		this.out = out;
		this.encoder = new HackedJsonEncoder(schema, out);
		this.writer = new GenericDatumWriter<T>(schema);
	}

	/**
	 * Write a datum to given output stream
	 * 
	 * @throws IOException
	 */
	public void write(T datum) throws IOException {
		writer.write(datum, encoder);
	}

	/**
	 * Close the stream
	 */
	@Override
	public void close() throws IOException {
		encoder.flush();
		out.flush();
		out.close();
	}
}
