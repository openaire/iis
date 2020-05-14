package eu.dnetlib.iis.common.java.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
     * Lists hadoop files located below pathname or alternatively lists subdirs under pathname.
     *
     * @param hadoopConf Configuration of hadoop env
     * @param pathname   Path to be listed for hadoop files
     * @return List with string locations of hadoop files
     */
    public static List<String> listFiles(Configuration hadoopConf, String pathname) throws IOException {
        return Arrays
                .stream(FileSystem.get(hadoopConf).listStatus(new Path(pathname)))
                .filter(FileStatus::isDirectory)
                .map(x -> x.getPath().toString())
                .collect(Collectors.toList());
    }

}
