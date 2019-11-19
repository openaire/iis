package eu.dnetlib.iis.wf.export.actionmanager;

import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.io.SequenceFileTextValueReader;
import eu.dnetlib.iis.common.java.stream.StreamUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Common RDD related test utility class.
 */
public class RDDTestUtils {

    private RDDTestUtils() {
    }

    /**
     * Reads a sequence file of Text values, applies a given function to each element and returns a list of results.
     *
     * @param location Path to read from.
     * @param mapper   Function to apply to each Text value.
     * @param <X>      Type of elements in result list.
     * @return List of results.
     * @throws IOException
     */
    public static <X> List<X> readValues(String location, Function<Text, X> mapper) throws IOException {
        return StreamUtils
                .withCloseableIterator(
                        new SequenceFileTextValueReader(new FileSystemPath(createLocalFileSystem(), new Path(new File(location).getAbsolutePath()))),
                        stream -> stream.map(mapper).collect(Collectors.toList()));
    }

    private static FileSystem createLocalFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        return FileSystem.get(conf);
    }
}