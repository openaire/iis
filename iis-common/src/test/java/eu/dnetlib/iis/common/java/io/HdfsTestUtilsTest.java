package eu.dnetlib.iis.common.java.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathFilter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class HdfsTestUtilsTest {

    @Test
    public void givenANotExistingPath_whenCountFilesIsCalled_thenExceptionIsThrown() {
        assertThrows(RuntimeException.class, () ->
                HdfsTestUtils.countFiles(new Configuration(), "/path/to/dir", mock(PathFilter.class)));
    }

    @Test
    public void givenAPathToFile_whenCountFilesIsCalled_thenExceptionIsThrown() throws IOException {
        Path tempFile = Files.createTempFile(this.getClass().getSimpleName(), "tmp");
        assertThrows(RuntimeException.class, () ->
                HdfsTestUtils.countFiles(new Configuration(), tempFile.toString(), mock(PathFilter.class)));
    }

    @Test
    public void givenAPathToDir_whenCountFilesIsCalledWithFilter_thenProperFileCountIsReturned() throws IOException {
        Path tempDir = Files.createTempDirectory(this.getClass().getSimpleName());
        Files.createTempFile(tempDir, "count_me", "a");
        Files.createTempFile(tempDir, "count_me", "a");
        Files.createTempFile(tempDir, "not_count_me", "b");

        int result = HdfsTestUtils.countFiles(new Configuration(), tempDir.toString(), x -> x.getName().endsWith("a"));

        assertEquals(2, result);
    }

    @Test
    @DisplayName("Count files properly counts files with an extension")
    public void givenAPathToDir_whenCountFilesIsCalledWithAnExtension_thenProperFileCountIsReturned(@TempDir Path tempDir) throws IOException {
        Files.createTempFile(tempDir, "count_me", "a");
        Files.createTempFile(tempDir, "count_me", "a");
        Files.createTempFile(tempDir, "not_count_me", "b");

        int result = HdfsTestUtils.countFiles(new Configuration(), tempDir.toString(), "a");

        assertEquals(2, result);
    }
}
