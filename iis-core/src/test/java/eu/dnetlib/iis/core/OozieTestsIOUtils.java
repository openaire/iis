package eu.dnetlib.iis.core;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.util.IOUtils;

/**
 * Provides Oozie-related IO methods
 * @author Mateusz Kobos
 *
 */
public class OozieTestsIOUtils {
	private FileSystem fs;
	private final static String confStorageDirProperty = "hadoop.log.dir";
	private final static String confStorageFileName = "shared_configuration.conf";
	
	public OozieTestsIOUtils(FileSystem fs) {
		this.fs = fs;
	}
	
	public void copyResourceToHDFS(String resourcePath, Path destination)
			throws IOException {
		InputStream in = IOUtils.getResourceAsStream(resourcePath, -1);
		OutputStream out = fs.create(destination);
		IOUtils.copyStream(in, out);
	}

	public void copyLocalToHDFS(File source, Path destination)
			throws IOException {
		Path srcPath = new Path(source.getAbsolutePath());
		fs.copyFromLocalFile(srcPath, destination);
	}
	
//	public void copyHDFSToLocal(Path source, File destination) 
//			throws IOException{
//		InputStream in = fs.open(source);
//		OutputStream out = new FileOutputStream(destination);
//		IOUtils.copyStream(in, out);
//	}
	
	/** Saves the configuration in a local working directory created by 
	 * Oozie test case for given test method. This method should be called
	 * from the inside of an Oozie test case method, otherwise its functioning
	 * is undefined. The saved information can be retrieved by calling 
	 * {@link loadConfiguration()}.
	 * 
	 * @param conf configuration that should be retrieved from the inside of
	 * the Oozie test case method by calling {@link createJobConf()}
	 * @throws IOException
	 */
	public static void saveConfiguration(Configuration conf) throws IOException{
		File f = new File(System.getProperty(confStorageDirProperty), 
				confStorageFileName);
		DataOutputStream out = new DataOutputStream(new FileOutputStream(f));
		conf.write(out);
		out.close();
	}
	
	/** A counterpart of the {@link saveConfiguration()} method. It should be
	 * called from the inside of a code run on the Hadoop created 
	 * by Oozie test case.
	 * 
	 * @return
	 * @throws IOException
	 */
	public static Configuration loadConfiguration() throws IOException{
		File f = new File(System.getProperty(confStorageDirProperty), 
				confStorageFileName);
		DataInputStream in = new DataInputStream(new FileInputStream(f));
		Configuration conf = new Configuration();
		conf.readFields(in);
		in.close();
		return conf;
	}
	
}
