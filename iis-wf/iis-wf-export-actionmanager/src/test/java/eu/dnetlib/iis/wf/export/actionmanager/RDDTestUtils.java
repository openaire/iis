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

public class RDDTestUtils {

    private RDDTestUtils() {
    }

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
