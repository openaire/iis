package eu.dnetlib.iis.core;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.io.Files;

import eu.dnetlib.iis.core.java.io.DataStore;
import eu.dnetlib.iis.core.java.io.FileSystemPath;

/**
 * Provides a facade hiding the complexities of communicating with
 * remote Oozie application and its data files placed in HDFS.
 * @author Mateusz Kobos
 * @author Mateusz Fedoryszak
 *
 */
public class RemoteOozieAppManager {
    private static final File primedClassDir = new File("target/primed");
	private static final String oozieAppDirName = "oozie_app";
	private static final String sandboxDirName = "working_dir";
	private static final StringReplacer replacer = new StringReplacer();

	private FileSystem hdfs;
	private String hdfsDir;
	private OozieTestsIOUtils ioUtils;
	private Path workingDir;
	private Path oozieAppPath;

	/**
	 * Initialize the object and upload the Oozie application placed in
	 * local resources to remote HDFS directory
	 * @param oozieAppLocalPath path to local Oozie application in resources
	 */
	private RemoteOozieAppManager(FileSystem hdfs, Path hdfsTestCaseDir,
			String oozieAppLocalPath) throws IOException{
        this(hdfs, hdfsTestCaseDir, new File(Thread.currentThread().getContextClassLoader()
                .getResource(oozieAppLocalPath).getPath()));
	}

    /**
     * Initialize the object and upload the Oozie application placed in
     * local file system to remote HDFS directory
     */
    private RemoteOozieAppManager(FileSystem hdfs, Path hdfsTestCaseDir,
                                 File oozieAppLocalFile) throws IOException{
        this.hdfs = hdfs;
        this.hdfsDir = hdfsTestCaseDir.toUri().getPath();
        this.ioUtils = new OozieTestsIOUtils(hdfs);
        /** "workingDir" is the directory in which all data consumed and produced
         * by Oozie application is stored. This directory also contains working
         * dirs of workflow nodes */
        this.workingDir = new Path(hdfsDir, sandboxDirName);
        this.hdfs.mkdirs(workingDir);
        this.oozieAppPath = new Path(hdfsDir, oozieAppDirName);
        copyOozieAppToHDFSWithOnTheFlightChangesHack(
                ioUtils, oozieAppLocalFile, oozieAppPath);
    }

    /**
     * Initialize the object and upload the Oozie application placed in
     * local primed class directory to remote HDFS directory.
     */
    public static RemoteOozieAppManager fromPrimedClassDir(FileSystem hdfs, Path hdfsTestCaseDir,
                                                           String oozieAppPackage) throws IOException {
        return new RemoteOozieAppManager(hdfs, hdfsTestCaseDir, new File(primedClassDir, oozieAppPackage));
    }

	/**
	 * Copy Oozie application to HDFS and replace some strings within XML
	 * workflow definitions.
	 *
	 * The replacements are needed in order to run the tests properly.
	 * @throws IOException
	 */
	private static void copyOozieAppToHDFSWithOnTheFlightChangesHack(
			OozieTestsIOUtils ioUtils, File srcAppPath,
			Path oozieAppPath) throws IOException{
		File appPath = Files.createTempDir();
		Iterator<File> fIterator = FileUtils.iterateFiles(srcAppPath,
				TrueFileFilter.TRUE, TrueFileFilter.TRUE);
		while(fIterator.hasNext()){
			File srcFile = fIterator.next();
			String relativePath = getRelativeFilePath(srcAppPath, srcFile);
			File destFile = new File(appPath, relativePath);
			destFile.getParentFile().mkdirs();
			if(!srcFile.getName().equals("workflow.xml")){
				FileUtils.copyFile(srcFile, destFile);
			} else {
				replacer.replace(srcFile, destFile);
			}
		}
		ioUtils.copyLocalToHDFS(appPath, oozieAppPath);
	}

	/**
	 * Get the part of the path that is relative with respect to the base path.
	 * We assume that the path points to the directory nested deeper than the
	 * base path
	 *
	 * @return given {@code base} path, split {@code path} into parts:
	 * {@code path = base + pathSeparator + relative} and return the
	 * relative par
	 */
	private static String getRelativeFilePath(File base, File path){
		String baseStr = base.getAbsolutePath();
		String pathStr = path.getAbsolutePath();
		if(!pathStr.startsWith(baseStr)){
			throw new RuntimeException(String.format(
					"Base path (\"%s\") doesn't match the begining of" +
					"the path \"%s\"", baseStr, pathStr));
		}
		String rest = pathStr.substring(baseStr.length());
		if(rest.length() != 0){
			if(rest.charAt(0) == File.separatorChar){
				rest = rest.substring(1);
			}
		}
		return rest;
	}

	public Path getWorkingDir(){
		return workingDir;
	}

	public Path getOozieAppPath(){
		return oozieAppPath;
	}

	public <T> List<T> readDataStoreFromWorkingDir(
			String name) throws IOException{
		List<T> records =
				DataStore.read(
					new FileSystemPath(hdfs, new Path(getWorkingDir(), name)));
		return records;
	}

	public void copyResourceFilesToWorkingDir(
			Map<String, String> resourcePathToHDFSFileNameMap) throws IOException{
		for(Map.Entry<String, String> entry:
				resourcePathToHDFSFileNameMap.entrySet()){
			Path hdfsFile = new Path(workingDir, entry.getValue());
			ioUtils.copyResourceToHDFS(entry.getKey(), hdfsFile);
		}
	}

	public void copyFilesFromWorkingDir(
			Map<String, File> hdfsFileNameToLocalFileNameMap) throws IOException{
		for(Map.Entry<String, File> entry:
				hdfsFileNameToLocalFileNameMap.entrySet()){
			File localFile = entry.getValue();
			Path hdfsFile = new Path(workingDir, entry.getKey());
			hdfs.copyToLocalFile(hdfsFile, new
					Path(localFile.getAbsolutePath()));
		}
	}
}
