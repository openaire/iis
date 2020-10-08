package eu.dnetlib.iis.common.java.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathFilter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class HdfsUtilsTest {

    @Test
    public void listDirsShouldThrowOnError() {
        // when
        assertThrows(Exception.class, () -> HdfsUtils.listDirs(new Configuration(), null));
    }

    @Test
    public void listDirsShouldListDirsLocatedInPath() throws IOException {
        Path tempDir = Files.createTempDirectory(this.getClass().getSimpleName());
        Path subDir1 = Files.createTempDirectory(tempDir, "list_me");
        Path subDir2 = Files.createTempDirectory(tempDir, "list_me");

        // when
        List<String> paths = HdfsUtils.listDirs(new Configuration(), tempDir.toString());

        // then
        assertEquals(2, paths.size());
        List<String> expecteds = Stream.of(subDir1.toString(), subDir2.toString())
                .map(path -> String.format("file:%s", path))
                .sorted().collect(Collectors.toList());
        List<String> actuals = paths.stream().sorted().collect(Collectors.toList());
        assertEquals(expecteds, actuals);
    }

    @Test
    public void givenANotExistingPath_whenCountFilesIsCalled_thenExceptionIsThrown() {
        assertThrows(RuntimeException.class, () ->
                HdfsUtils.countFiles(new Configuration(), "/path/to/dir", mock(PathFilter.class)));
    }

    @Test
    public void givenAPathToFile_whenCountFilesIsCalled_thenExceptionIsThrown() throws IOException {
        Path tempFile = Files.createTempFile(this.getClass().getSimpleName(), "tmp");
        assertThrows(RuntimeException.class, () ->
                HdfsUtils.countFiles(new Configuration(), tempFile.toString(), mock(PathFilter.class)));
    }

    @Test
    public void givenAPathToDir_whenCountFilesIsCalledWithFilter_thenProperFileCountIsReturned() throws IOException {
        Path tempDir = Files.createTempDirectory(this.getClass().getSimpleName());
        Files.createTempFile(tempDir, "count_me", "a");
        Files.createTempFile(tempDir, "count_me", "a");
        Files.createTempFile(tempDir, "not_count_me", "b");

        int result = HdfsUtils.countFiles(new Configuration(), tempDir.toString(), x -> x.getName().endsWith("a"));

        assertEquals(2, result);
    }
}