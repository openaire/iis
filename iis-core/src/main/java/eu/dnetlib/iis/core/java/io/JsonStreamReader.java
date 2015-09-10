package eu.dnetlib.iis.core.java.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.HackedJsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;

/**
 * Read JSON stream corresponding to a data store. Single line in the input
 * is a single record stored in JSON format.
 * @author Mateusz Kobos
 */
public class JsonStreamReader<T> implements CloseableIterator<T>{

	private final InputStream in;
	private final DatumReader<T> reader;
	private final Decoder decoder;
	private T nextRecord;
	
	public JsonStreamReader(Schema schema, InputStream input, 
			Class<T> recordType) throws IOException{
		if(recordType == GenericRecord.class){
			this.reader = new GenericDatumReader<T>(schema);
		} else {
			this.reader = new SpecificDatumReader<T>(schema);
		}
		this.in = input;
		this.decoder = new HackedJsonDecoder(schema, in);
		this.nextRecord = null;
		readNext();	
	}
	
	private void readNext() throws IOException{
		try {
			nextRecord = reader.read(null, decoder);
		} catch(EOFException ex){
			nextRecord = null;
			in.close();
		}
	}
	
	@Override
	public boolean hasNext() {
		return nextRecord != null;
	}

	@Override
	public T next() {
		T current = nextRecord;
		try {
			readNext();
		} catch (IOException e) {
			new RuntimeException(e);
		}
		return current;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();		
	}

	@Override
	public void close() throws IOException {
		in.close();		
	}

}
