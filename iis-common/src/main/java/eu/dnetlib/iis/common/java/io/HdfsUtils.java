package eu.dnetlib.iis.common.java.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Util class containing operations on hdfs or local filesystem
 *
 * @author madryk
 */
public final class HdfsUtils {

    //------------------------ CONSTRUCTORS -------------------

    private HdfsUtils() {
    }

    //------------------------ LOGIC --------------------------

    /**
     * Removes file or directory (recursively) located under the specified pathname.
     */
    public static void remove(Configuration hadoopConf, String pathname) throws IOException {
        Path path = new Path(pathname);
        FileSystem fileSystem = FileSystem.get(hadoopConf);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }

    /**
     * Lists subdirs under pathname.
     *
     * @param hadoopConf Configuration of hadoop env
     * @param pathname   Path to be listed for subdirs
     * @return List with string locations of subdirs
     */
    public static List<String> listDirs(Configuration hadoopConf, String pathname) throws IOException {
        return Arrays
                .stream(FileSystem.get(hadoopConf).listStatus(new Path(pathname)))
                .filter(FileStatus::isDirectory)
                .map(x -> x.getPath().toString())
                .collect(Collectors.toList());
    }

    /**
     * Counts files in a dir.
     *
     * @param hadoopConf Configuration of hadoop env
     * @param pathname   Path to a dir with files
     * @param pathFilter Filter for files to be counted
     * @return File count of files matching the filter
     */
    public static int countFiles(Configuration hadoopConf, String pathname, PathFilter pathFilter) throws IOException {
        Path path = new Path(pathname);
        FileSystem fileSystem = FileSystem.get(hadoopConf);
        if (fileSystem.exists(path) && fileSystem.isDirectory(path)) {
            return fileSystem.listStatus(path, pathFilter).length;
        }
        throw new RuntimeException(String.format("Path does not exist or is not a directory: %s", pathname));
    }

    /**
     * Counts files in a dir.
     *
     * @param hadoopConf Configuration of hadoop env
     * @param pathname   Path to a dir with files
     * @param extension  Extension of files to be counted
     * @return File count of files with the extension
     */
    public static int countFiles(Configuration hadoopConf, String pathname, String extension) throws IOException {
        return countFiles(hadoopConf, pathname, path -> path.getName().endsWith(extension));
    }
}
