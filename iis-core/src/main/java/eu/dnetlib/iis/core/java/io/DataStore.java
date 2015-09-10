package eu.dnetlib.iis.core.java.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;


/**
 * Utility for accessing to Avro-based data stores stored in file system
 * @author Mateusz Kobos
 *
 */
public final class DataStore {
	
	private final static String singleDataStoreFileName = "content.avro";
	
	private DataStore(){}

	/**
	 * Create a new data store directory and return writer that allows 
	 * adding new records
	 * @param path path to a directory to be created
	 * @param schema schema of the records to be stored in the file
	 * @return 
	 * @throws IOException
	 */
	public static <T> DataFileWriter<T> create(
			FileSystemPath path, Schema schema) throws IOException{
		path.getFileSystem().mkdirs(path.getPath());
		FileSystemPath outFile = new FileSystemPath(
				path, singleDataStoreFileName);
		return createSingleFile(outFile, schema);
	}
	
	/**
	 * Get reader for reading records from given data store
	 * 
	 * Here the schema used for reading the data store is set to be the same 
	 * as the one that was used to write it.
	 * 
	 * @see getReader(FileSystemPath path, Schema readerSchema) for details.
	 * 
	 */
	public static <T> CloseableIterator<T> getReader(FileSystemPath path) 
			throws IOException{
		return getReader(path, null);
	}
	
	/**
	 * Get reader for reading records from given data store
	 * @param path path to a directory corresponding to data store
	 * @param readerSchema the schema onto which the read data store will 
	 * 	be projected
	 */
	public static <T> CloseableIterator<T> getReader(
			FileSystemPath path, Schema readerSchema) throws IOException{
		return new AvroDataStoreReader<T>(path, readerSchema);
	}
	
	/**
	 * Read data store entries and insert them into a list. A utility function.
	 * 
	 * Here the schema used for reading the data store is set to be the same 
	 * as the one that was used to write it.
	 */
	public static <T> List<T> read(FileSystemPath path) 
			throws IOException{
		return read(path, null);
	}
	
	/**
	 * Read data store entries and insert them into a list. A utility function.
	 * 
	 * @param readerSchema the schema onto which the read data store will 
	 * 	be projected
	 */
	public static <T> List<T> read(FileSystemPath path, Schema readerSchema) 
			throws IOException{
		CloseableIterator<T> iterator = getReader(path, readerSchema);
		List<T> elems = new ArrayList<T>();
		while(iterator.hasNext()){
			elems.add(iterator.next());
		}
		return elems;
	}
	
	/**
	 * Create a data store from a list of entries. A utility function.
	 * The schema is implicitly
	 * taken from the first element from the {@code elements} list.
	 * @param elements list of elements to write. At least one element has
	 * to be present, because it is used to retrieve schema of the
	 * structures passed in the list.
	 */
	public static <T extends GenericContainer> void create(
			List<T> elements, FileSystemPath path) throws IOException{
		if(elements.size() == 0){
			throw new IllegalArgumentException(
					"The list of elements has to be non-empty");
		}
		Schema schema = elements.get(0).getSchema();
		create(elements, path, schema);
	}

	/**
	 * Create a data store from a list of entries with schema given explicitly.
	 * A utility function.
	 */
	public static <T extends GenericContainer> void create(
			List<T> elements, FileSystemPath path, Schema schema) 
					throws IOException{
		DataFileWriter<T> writer = create(path, schema);
		try{
			for(T i: elements){
				writer.append(i);
			}
		} finally {
			if(writer != null){
				writer.close();
			}
		}
	}
	
	/**
	 * Create a single Avro file. This method shouldn't be normally used to
	 * create data stores since it creates only a single Avro file, 
	 * while a data store consists of a directory containing one or more files.
	 */
	public static <T> DataFileWriter<T> createSingleFile(
			FileSystemPath path, Schema schema) throws IOException{
		DatumWriter<T> datumWriter = new SpecificDatumWriter<T>();
		DataFileWriter<T> writer = new DataFileWriter<T>(datumWriter);
		writer.create(schema, path.getFileSystem().create(path.getPath()));
		return writer;
	}

}
