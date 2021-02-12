package eu.dnetlib.iis.common.java.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;

public class HdfsTestUtils {

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
