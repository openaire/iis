package eu.dnetlib.iis.common.java.io;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Path to a directory or a file along with information about the 
 * file system in which the path is defined.
 * 
 * @author Mateusz Kobos
 *
 */
public class FileSystemPath {
	private final FileSystem fs;
	private final Path path;

	/**
	 * Path in the local file system
	 */
	public FileSystemPath(File file) throws IOException {
		this(new Path(file.toURI()));
	}
	
	/**
	 * Path in the local file system
	 */
	public FileSystemPath(Path path) throws IOException{
		this(FileSystem.get(new Configuration(false)), path);
	}
	
	/**
	 * Path in the given file system
	 */
	public FileSystemPath(FileSystem fs, Path path){
		this.fs = fs;
		this.path = path;
	}
	
	/** Create a path with a child element */
	public FileSystemPath(FileSystemPath parent, String child){
		this.fs = parent.getFileSystem();
		this.path = new Path(parent.getPath(), child);
	}

	public FileSystem getFileSystem() {
		return fs;
	}

	public Path getPath() {
		return path;
	}
	
	public FSDataInputStream getInputStream() throws IOException{
		return getFileSystem().open(getPath());
	}
}
