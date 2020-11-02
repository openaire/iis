package eu.dnetlib.iis.common.java.io;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Iterator that extracts sequence file's consecutive {@link Text} values.
 * 
 * @author mhorst
 */
public class SequenceFileTextValueReader implements CloseableIterator<Text> {

	private SequenceFile.Reader sequenceReader;

	private final RemoteIterator<LocatedFileStatus> fileIt;

	private final FileSystem fs;

	/**
	 * Ignore file starting with underscore. Such files are also ignored by
	 * default by map-reduce jobs.
	 */
	private final static Pattern WHITELIST_REGEXP = Pattern.compile("^[^_].*");

	private Text toBeReturned;

	//------------------------ CONSTRUCTORS --------------------------
	
	/**
	 * Default constructor.
	 * 
	 * @param path HDFS path along with associated FileSystem
	 * @throws IOException
	 */
	public SequenceFileTextValueReader(final FileSystemPath path) throws IOException {
		this.fs = path.getFileSystem();
		if (fs.isDirectory(path.getPath())) {
			fileIt = fs.listFiles(path.getPath(), false);
			sequenceReader = getNextSequenceReader();
		} else {
			fileIt = null;
			sequenceReader = new Reader(fs.getConf(), SequenceFile.Reader.file(path.getPath()));
		}
	}

	//------------------------ LOGIC ---------------------------------
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		// check and provide next when already returned
		if (toBeReturned == null) {
			toBeReturned = getNext();
		}
		return toBeReturned != null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Iterator#next()
	 */
	@Override
	public Text next() {
		if (toBeReturned != null) {
			// element fetched while executing hasNext()
			Text result = toBeReturned;
			toBeReturned = null;
			return result;
		} else {
			Text resultCandidate = getNext();
			if (resultCandidate!=null) {
				return resultCandidate;
			} else {
				throw new NoSuchElementException();
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.dnetlib.iis.exp.iterator.ClosableIterator#close()
	 */
	@Override
	public void close() throws IOException {
		if (sequenceReader != null) {
			sequenceReader.close();
		}
	}
	
	//------------------------ PRIVATE -------------------------------
	
	private final Reader getNextSequenceReader() throws IOException {
		while (fileIt != null && fileIt.hasNext()) {
			LocatedFileStatus currentFileStatus = fileIt.next();
			if (isValidFile(currentFileStatus)) {
				return new Reader(this.fs.getConf(), SequenceFile.Reader.file(currentFileStatus.getPath()));
			}
		}
		// fallback
		return null;
	}

	/**
	 * Checks whether file is valid candidate.
	 * 
	 * @param fileStatus
	 *            file status holding file name
	 * @return true when valid, false otherwise
	 */
	private final boolean isValidFile(LocatedFileStatus fileStatus) {
		if (fileStatus.isFile()) {
			return WHITELIST_REGEXP.matcher(fileStatus.getPath().getName()).matches();
		} else {
			return false;
		}
	}

	/**
	 * @return next data package
	 */
	private Text getNext() {
		try {
			if (sequenceReader == null) {
				return null;
			}
			Writable key = (Writable) ReflectionUtils.newInstance(sequenceReader.getKeyClass(), fs.getConf());
			Writable value = (Writable) ReflectionUtils.newInstance(sequenceReader.getValueClass(), fs.getConf());
			if (sequenceReader.next(key, value)) {
				return (Text) value;
			} else {
				sequenceReader.close();
				sequenceReader = getNextSequenceReader();
				if (sequenceReader != null) {
					return getNext();
				}
			}
			// fallback
			return null;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static SequenceFileTextValueReader fromFile(String pathname) throws IOException {
		return new SequenceFileTextValueReader(new FileSystemPath(new File(pathname)));
	}
}