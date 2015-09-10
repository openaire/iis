package eu.dnetlib.iis.core.java.io;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;



/**
 * An abstraction over data store format which allows
 * iterating over records stored in the data store. 
 * It handles the standard case of a data store that is a directory containing 
 * many Avro files (but it can also read records from a single file). 
 * 
 * @author mhorst
 * @author Mateusz Kobos
 */
class AvroDataStoreReader<T> implements CloseableIterator<T> {

	private DataFileReader<T> currentReader;
	private RemoteIterator<LocatedFileStatus> fileIterator;
	private final FileSystemPath path;
	private final Schema readerSchema;

	/**
	 * Ignore file starting with underscore. Such files are also ignored by
	 * default by map-reduce jobs.
	 */
	private final Pattern whitelistPattern = Pattern.compile("^(?!_).*");

	/**
	 * Here the schema used for reading the data store is set to be the same 
	 * as the one that was used to write it.
	 */
	public AvroDataStoreReader(final FileSystemPath path)
			throws IOException {
		this(path, null);
	}
	
	/**
	 * @param path path to the data store to be read
	 * @param readerSchema the schema onto which the read data store will 
	 * 	be projected
	 */
	public AvroDataStoreReader(final FileSystemPath path, Schema readerSchema)
			throws IOException {
		this.path = path;
		this.readerSchema = readerSchema;
		fileIterator = path.getFileSystem().listFiles(path.getPath(), false);
		currentReader = getNextNonemptyReader();
	}
	
	private DataFileReader<T> getNextNonemptyReader() throws IOException {
		while (fileIterator != null && fileIterator.hasNext()) {
			LocatedFileStatus currentFileStatus = fileIterator.next();
			if (isValidFile(currentFileStatus)) {
				FileSystemPath currPath = new FileSystemPath(
						path.getFileSystem(), currentFileStatus.getPath());
				DataFileReader<T> reader = 
						getSingleFileReader(currPath, readerSchema);
				/** Check if the file contains at least one record */
				if(reader.hasNext()){
					return reader;
				} else {
					reader.close();
				}
			}
		}
		/** fallback */
		return null;
	}
	
	/**
	 * Get a reader for the specified Avro file. A utility function. 
	 * @param path path to the existing file
	 * @param readerSchema optional reader schema. If you want to use the
	 *		default option of using writer schema as the reader schema, pass the
	 *		{@code null} value. 
	 * @throws IOException
	 */
	private static <T> DataFileReader<T> getSingleFileReader(
			FileSystemPath path, Schema readerSchema) throws IOException{
		try{
		SpecificDatumReader<T> datumReader = new SpecificDatumReader<T>();
		if(readerSchema != null){
			datumReader.setExpected(readerSchema);
		}
		long len = path.getFileSystem().getFileStatus(path.getPath()).getLen();
		FSDataInputStream inputStream = path.getFileSystem().open(path.getPath());
		return new DataFileReader<T>(
				new AvroFSInput(inputStream, len), datumReader);
		} catch (IOException ex){
			throw new IOException("Problem with file \""+
					path.getPath().toString()+"\": "+ex.getMessage(), ex);
		}
	}

	/**
	 * Checks whether file is valid
	 * 
	 * @param fileStatus
	 * @return true when valid, false otherwise
	 */
	private boolean isValidFile(LocatedFileStatus fileStatus) {
		if (fileStatus.isFile()) {
			return whitelistPattern.matcher(
					fileStatus.getPath().getName()).matches();
		}
		/** fallback */
		return false;
	}

	@Override
	public boolean hasNext() {
		return currentReader != null;
	}

	@Override
	public T next(){
		if(currentReader == null){
			throw new NoSuchElementException();
		}
		T obj = currentReader.next();
		if(!currentReader.hasNext()){
			try{
				currentReader.close();
				currentReader = getNextNonemptyReader();
			} catch(IOException ex){
				throw new RuntimeException(ex);
			}
		}
		return obj;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws IOException {
		if(currentReader != null){
			currentReader.close();
			currentReader = null;
		}
		fileIterator = null;
	}
}